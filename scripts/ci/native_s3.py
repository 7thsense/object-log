#!/usr/bin/env python3
"""Run every mandatory S3 selector against an owned checksum-pinned RustFS."""
import argparse
import ctypes
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[2]
VERSION = "1.0.0"
URL = "https://github.com/rustfs/rustfs/releases/download/1.0.0/rustfs-linux-x86_64-musl-v1.0.0.zip"
SHA256 = "c30a95b76546f25122c9ca387090ddb30c391ca5605621b0d7c881703c0f21c8"
SELECTORS = (
    ("--lib", "native_v2::index::tests::real_s3_runs_identical_full_root_contract_in_both_signing_modes"),
    ("--lib", "native_v2::data::tests::real_s3_runs_identical_frame_contract_in_both_signing_modes"),
    ("--lib", "native_v2::head::tests::real_s3_runs_identical_captured_append_contract_in_both_signing_modes"),
    ("--lib", "native_v2::head::tests::real_s3_runs_identical_group_contract_in_both_signing_modes"),
    ("--lib", "native_v2::provision::tests::real_s3_runs_identical_provision_contract_in_both_signing_modes"),
    ("--lib", "native_v2::head::tests::real_s3_lost_response_processing_resolves_after_later_append_without_resubmission"),
    ("--lib", "native_v2::head::tests::coupled::coupled_s3_atomic_state_work_orphans_and_lost_reply_both_signing_modes"),
    ("--test", "native_facade", "external_s3_common_append_reopen_contract_both_signing_modes"),
    ("--test", "native_semantic", "public_semantic_s3_coupled_state_and_scan_both_signing_modes"),
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


_cancelling = False


def termination(_number, _frame):
    global _cancelling
    if _cancelling:
        return
    _cancelling = True
    raise KeyboardInterrupt("qualification terminated")


def own_descendants():
    # The pinned Linux fixture runner adopts and reaps its own orphaned test
    # descendants; it does not signal unrelated server or cargo processes.
    require(sys.platform == "linux", "pinned RustFS qualification requires Linux")
    control = ctypes.CDLL(None, use_errno=True).prctl
    control.argtypes = [ctypes.c_int] + [ctypes.c_ulong] * 4
    control.restype = ctypes.c_int
    require(control(36, 1, 0, 0, 0) == 0,
            "cannot establish qualification descendant ownership")
    signal.signal(signal.SIGTERM, termination)
    signal.signal(signal.SIGINT, termination)


CANCELLATION_SIGNALS = {signal.SIGINT, signal.SIGTERM}


def child_signals():
    # This standalone Linux runner is single-threaded. Children must not inherit
    # the parent's temporary registration/cleanup signal mask across exec.
    signal.pthread_sigmask(signal.SIG_UNBLOCK, CANCELLATION_SIGNALS)


def drain_group(process):
    def send(number):
        try:
            os.killpg(process.pid, number)
        except ProcessLookupError:
            pass
    send(signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        pass
    send(signal.SIGKILL)
    process.wait()
    deadline = time.monotonic() + 5
    while True:
        try:
            child, _ = os.waitpid(-process.pid, os.WNOHANG)
        except ChildProcessError:
            return
        if child == 0:
            require(time.monotonic() < deadline, "owned command descendants did not drain")
            time.sleep(0.01)


@contextmanager
def owned_process(command, *, _registered=None, **options):
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
    try:
        process = subprocess.Popen(command, start_new_session=True,
                                   preexec_fn=child_signals, **options)
        try:
            if _registered is not None:
                _registered(process)
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            yield process
        finally:
            signal.pthread_sigmask(signal.SIG_BLOCK, CANCELLATION_SIGNALS)
            drain_group(process)
    finally:
        # Pending cancellation is delivered only after the complete owned group
        # has drained, including a second signal received during cleanup.
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


def run_owned(command, env):
    with owned_process(command, cwd=ROOT, env=env, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT) as process:
        output, _ = process.communicate()
        return process.returncode, output


def test(command, env, expected):
    code, output = run_owned(command, env)
    print(output, end="", flush=True)
    summaries = re.findall(r"test result: ok\. (\d+) passed; 0 failed; 0 ignored;", output)
    require(code == 0 and summaries == [str(expected)],
            "mandatory S3 qualification did not execute its exact positive test count")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="exercise failure and cancellation controls before qualification")
    parser.add_argument("--archive", type=Path, help="use a local archive, still requiring the pinned SHA256")
    args = parser.parse_args()
    own_descendants()
    if args.self_test:
        from native_s3_controls import run
        run(sys.modules[__name__])
    with tempfile.TemporaryDirectory(prefix="object-log-rustfs-") as raw:
        directory = Path(raw)
        archive = args.archive or directory / "rustfs.zip"
        if args.archive is None:
            with urllib.request.urlopen(URL, timeout=60) as response, archive.open("wb") as output:
                while chunk := response.read(1 << 20):
                    output.write(chunk)
        with archive.open("rb") as source:
            require(hashlib.file_digest(source, "sha256").hexdigest() == SHA256,
                    "RustFS release archive checksum mismatch")
        binary = directory / "rustfs"
        with zipfile.ZipFile(archive) as package:
            with package.open("rustfs") as source, binary.open("wb") as output:
                while chunk := source.read(1 << 20):
                    output.write(chunk)
        binary.chmod(0o700)
        code, version = run_owned([str(binary), "--version"], os.environ.copy())
        require(code == 0 and re.search(r"\brustfs " + re.escape(VERSION) + r"(?:\s|$)", version), "unexpected RustFS version")
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        endpoint = f"http://127.0.0.1:{port}"
        env = os.environ.copy()
        # Public credentials belong only to this fresh loopback test server.
        env.update(RUSTFS_ACCESS_KEY="objectlog-ci", RUSTFS_SECRET_KEY="objectlog-ci-secret",
                   RUSTFS_CONSOLE_ENABLE="false", OBJECT_LOG_S3_ENDPOINT=endpoint,
                   OBJECT_LOG_S3_BUCKET="object-log-ci", OBJECT_LOG_S3_REGION="us-east-1",
                   OBJECT_LOG_S3_KEY_ID="objectlog-ci", OBJECT_LOG_S3_SECRET="objectlog-ci-secret",
                   OBJECT_LOG_S3_PROVIDER="rustfs")
        data = directory / "data"
        data.mkdir()
        with (directory / "server.log").open("w+", errors="replace") as log:
            with owned_process([str(binary), "server", str(data), "--address", f"127.0.0.1:{port}"],
                               env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT) as server:
                try:
                    deadline = time.monotonic() + 60
                    while True:
                        require(server.poll() is None, "owned RustFS exited before readiness")
                        try:
                            with urllib.request.urlopen(endpoint + "/health/ready", timeout=1) as response:
                                ready = response.status == 200
                        except (OSError, urllib.error.URLError):
                            ready = False
                        if ready:
                            require(server.poll() is None, "owned RustFS exited during readiness")
                            break
                        require(time.monotonic() < deadline, "owned RustFS readiness timed out")
                        time.sleep(0.1)
                    code, response = run_owned(["curl", "--fail", "--silent", "--show-error", "--max-time", "15",
                                    "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", "objectlog-ci:objectlog-ci-secret",
                                    "--request", "PUT", "--data", "", endpoint + "/object-log-ci"], env)
                    require(code == 0, "signed fixture bucket creation failed: " + response)
                    print(json.dumps({"provider": "rustfs", "version": VERSION, "archive_sha256": SHA256,
                                      "owned_pid": server.pid, "endpoint": endpoint}), flush=True)
                    test(["cargo", "test", "--features", "native,s3", "--test", "s3", "--", "--nocapture"], env, 3)
                    for selector in SELECTORS:
                        test(["cargo", "test", "--features", "native,s3", *selector,
                              "--", "--ignored", "--exact"], env, 1)
                    require(server.poll() is None, "owned RustFS exited during qualification")
                    print("PASS: 3 S3 adapter tests and 9 mandatory native provider selectors", flush=True)
                except BaseException:
                    log.flush()
                    log.seek(0, os.SEEK_END)
                    log.seek(max(0, log.tell()-16384))
                    print(log.read(), flush=True)
                    raise


if __name__ == "__main__":
    main()
