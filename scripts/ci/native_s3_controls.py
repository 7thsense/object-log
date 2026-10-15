"""Failure and cancellation controls for the mandatory owned S3 runner."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import zipfile


def run(runner):
    valid = "test result: ok. 1 passed; 0 failed; 0 ignored;"
    for output, code in [("test result: ok. 0 passed; 0 failed; 0 ignored;", 0),
                         ("test result: ok. 0 passed; 0 failed; 1 ignored;", 0),
                         (valid + "\n" + valid, 0), (valid, 1)]:
        try:
            runner.test([sys.executable, "-c", f"print({output!r}); raise SystemExit({code})"], os.environ.copy(), 1)
        except RuntimeError:
            pass
        else:
            raise RuntimeError("mandatory count/status negative control escaped")
    runner.test([sys.executable, "-c", f"print({valid!r})"], os.environ.copy(), 1)
    registered = []

    def pending(process):
        registered.append(process.pid)
        os.kill(os.getpid(), signal.SIGTERM)

    try:
        with runner.owned_process([sys.executable, "-c", "import time; time.sleep(60)"], _registered=pending):
            raise RuntimeError("pending spawn-registration signal was not delivered")
    except KeyboardInterrupt:
        pass
    finally:
        runner._cancelling = False  # Only this intentional, fully drained control recovers.
    runner.require(registered and not Path(f"/proc/{registered[0]}").exists(),
                   "spawn-registration cancellation leaked its child")
    with tempfile.TemporaryDirectory(prefix="native-s3-controls-") as raw:
        directory = Path(raw)
        bad = directory / "bad.zip"
        executed = directory / "unexpected-execution"
        with zipfile.ZipFile(bad, "w") as archive:
            archive.writestr("rustfs", f'#!/usr/bin/env python3\nfrom pathlib import Path\nPath({str(executed)!r}).write_text("executed")\nprint("rustfs 1.0.0")\n')
        code, failure = runner.run_owned([sys.executable, str(Path(runner.__file__)), "--archive", str(bad)], os.environ.copy())
        runner.require(code != 0 and "checksum mismatch" in failure and not executed.exists(),
                       "bad artifact did not fail before extraction/execution")
        for mode in ("selector-failure", "termination"):
            ready = directory / (mode + ".json")
            cleanup = directory / (mode + ".cleanup")
            grand_ready = directory / (mode + ".grandchild")
            worker = directory / (mode + ".py")
            grandchild = f'import signal,time; from pathlib import Path; signal.signal(signal.SIGTERM,signal.SIG_IGN); Path({str(grand_ready)!r}).write_text("ready"); time.sleep(60)'
            child = f'''
import json,os,signal,subprocess,sys,time
from pathlib import Path
signal.signal(signal.SIGTERM,lambda *_:Path({str(cleanup)!r}).write_text("cleanup started"))
grandchild=subprocess.Popen([sys.executable,"-c",{grandchild!r}],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
while not Path({str(grand_ready)!r}).exists():time.sleep(0.01)
Path({str(ready)!r}).write_text(json.dumps({{"child":os.getpid(),"grandchild":grandchild.pid,"server":SERVER_PID}}))
MODE_ACTION
'''
            action = 'print("test result: ok. 0 passed; 0 failed; 0 ignored;",flush=True); raise SystemExit(1)' if mode == "selector-failure" else "time.sleep(60)"
            worker.write_text(f'''
import os,subprocess,sys
sys.path.insert(0,{str(Path(runner.__file__).parent)!r})
from native_s3 import own_descendants,owned_process,test
own_descendants()
with owned_process([sys.executable,"-c","import time; time.sleep(60)"]) as server:
    child={child!r}.replace("SERVER_PID",str(server.pid)).replace("MODE_ACTION",{action!r})
    test([sys.executable,"-c",child],os.environ.copy(),1)
''')
            owned = {}
            with runner.owned_process([sys.executable, str(worker)], stdout=subprocess.PIPE,
                                      stderr=subprocess.STDOUT, text=True) as process:
                try:
                    deadline = time.monotonic() + 10
                    while not owned:
                        try:
                            owned = json.loads(ready.read_text())
                        except (OSError, ValueError):
                            runner.require(time.monotonic() < deadline and process.poll() is None,
                                           "lifecycle control failed before active child")
                            time.sleep(0.01)
                    if mode == "termination":
                        process.send_signal(signal.SIGTERM)
                        while not cleanup.exists():
                            runner.require(time.monotonic() < deadline and process.poll() is None,
                                           "cleanup control never entered active drainage")
                            time.sleep(0.01)
                        process.send_signal(signal.SIGTERM)
                        process.send_signal(signal.SIGINT)
                    output, _ = process.communicate(timeout=10)
                    runner.require(process.returncode != 0, "lifecycle control unexpectedly succeeded")
                    for pid in owned.values():
                        runner.require(not Path(f"/proc/{pid}").exists(),
                                       f"{mode} leaked owned process {pid}: {output}")
                finally:
                    previous = signal.pthread_sigmask(signal.SIG_BLOCK, runner.CANCELLATION_SIGNALS)
                    try:
                        # A failed control may orphan separate worker-owned
                        # sessions. Clean only their recorded groups and reap.
                        for name in ("child", "server"):
                            if name in owned:
                                try:
                                    os.killpg(owned[name], signal.SIGKILL)
                                except ProcessLookupError:
                                    pass
                                deadline = time.monotonic() + 5
                                while True:
                                    try:
                                        child_pid, _ = os.waitpid(-owned[name], os.WNOHANG)
                                    except ChildProcessError:
                                        break
                                    if child_pid == 0:
                                        runner.require(time.monotonic() < deadline, "control cleanup did not drain")
                                        time.sleep(0.01)
                    finally:
                        signal.pthread_sigmask(signal.SIG_SETMASK, previous)
    print("PASS: count/status, checksum, spawn-registration, failed-selector and repeated-cancellation controls", flush=True)
