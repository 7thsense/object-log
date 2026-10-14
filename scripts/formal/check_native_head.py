#!/usr/bin/env python3
"""Captured-model execution and failure-safe, target-owned authority evidence."""
from pathlib import Path
import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import types

ROOT = Path(__file__).resolve().parents[2]
MODEL = ROOT / 'scripts/formal/native_head_model.py'
DEFAULT = ROOT / 'target/native-head-model.json'
SOURCES = (
    'Cargo.toml', 'Cargo.lock', 'src/lib.rs', 'src/blob.rs', 'src/blob/authority.rs',
    'src/s3.rs', 'src/s3/authority.rs', 'src/sequencer.rs', 'src/native_head.rs',
    'src/native_head/tests.rs', 'src/native_head/tests/s3.rs',
    'scripts/formal/native_head_model.py', 'scripts/formal/check_native_head.py',
    'docs/helix/02-design/contracts/CONTRACT-002-object-store-api.md',
    'docs/helix/02-design/contracts/CONTRACT-001-core-log-api.md',
    '.github/workflows/ci.yml',
)
CODE_MAPPING = {
    'scope': 'MetadataSnapshot::validate provider/key tokens',
    'budget': 'Local read_local; S3 MetadataResponse/read_metadata_inner (physical read bounds are tested separately)',
    'root_preservation': 'test-internal Owner::execute epoch-only encoding',
    'ambiguous_retry': 'S3 one-attempt conditional PUT; head errors propagate',
    'physical_cancel': 'AppendOwner in Local blocking closures',
    'source_lifetime': 'MetadataSnapshot last owner field; OwnedMetadataBody carrier',
    'notify_order': 'PhysicalOwner permit release precedes owned/notify',
    'conflict_floor': 'Owner::execute retains prior head across conflict',
}

def owned_output(raw):
    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    target = ROOT / 'target'
    if not path.is_relative_to(target) or path.suffix != '.json':
        raise ValueError('evidence output must be a JSON file inside this checkout target/')
    relative = path.relative_to(ROOT)
    current = ROOT
    for component in relative.parts:
        if component in {'.', '..'}:
            raise ValueError('evidence path must not traverse directories')
        current = current / component
        if current.is_symlink():
            raise ValueError('evidence path must not contain symlinks')
    if not path.resolve().is_relative_to(target.resolve()):
        raise ValueError('evidence path escapes target/')
    return path

def atomic_report(path, value):
    owned_output(str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    owned_output(str(path))
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + '.', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as output:
            json.dump(value, output, indent=2)
            output.write('\n')
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)

def capture():
    return {
        'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'status': subprocess.check_output(['git', 'status', '--porcelain=v1', '-uall'], cwd=ROOT, text=True),
        'source_sha256': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES},
    }

def loaded_model(raw):
    module = types.ModuleType('native_head_captured_model')
    module.__file__ = str(MODEL)
    sys.modules[module.__name__] = module
    exec(compile(raw, str(MODEL), 'exec'), module.__dict__)
    return module

def failure_controls():
    DEFAULT.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='native-head-gate-controls-', dir=DEFAULT.parent) as folder:
        report = Path(folder) / 'report.json'
        def run(extra=()):
            return subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output', str(report), *extra], cwd=ROOT, capture_output=True, text=True)
        controls = []
        for label, extra in [('failed-model', ('--negative-control', 'conflict_floor')), ('bad-cli', ('--invalid-option',)), ('abbreviated-output', ('--out', str(report)))]:
            assert run().returncode == 0
            prior = json.loads(report.read_text())
            assert prior['status'] == 'PASS' and prior['passed']
            failed = run(extra)
            terminal = json.loads(report.read_text())
            assert failed.returncode != 0 and terminal['status'] == 'FAIL' and not terminal['passed']
            controls.append({'control': label, 'prior_genuine_pass': True, 'failed_run_exit': failed.returncode, 'terminal': terminal['status']})
        sentinel = Path(folder) / 'sentinel.txt'
        sentinel.write_text('unchanged')
        invalid = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output', str(sentinel)], cwd=ROOT, capture_output=True, text=True)
        assert invalid.returncode != 0 and sentinel.read_text() == 'unchanged'
        link = Path(folder) / 'link.json'
        link.symlink_to(sentinel)
        invalid = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output', str(link)], cwd=ROOT, capture_output=True, text=True)
        assert invalid.returncode != 0 and sentinel.read_text() == 'unchanged'
        controls.append({'control': 'non-json-and-symlink-rejection', 'sentinel_unchanged': True})
        with tempfile.TemporaryDirectory(prefix='native-head-external-control-') as external:
            outside = Path(external) / 'sentinel.json'
            outside.write_text('{"sentinel":"unchanged"}')
            digest = hashlib.sha256(outside.read_bytes()).hexdigest()
            invalid = subprocess.run([sys.executable, str(Path(__file__).resolve()), '--output', str(outside)], cwd=ROOT, capture_output=True, text=True)
            assert invalid.returncode != 0 and hashlib.sha256(outside.read_bytes()).hexdigest() == digest
        controls.append({'control': 'outside-target-rejection', 'sentinel_unchanged': True})
        return controls

def main(argv=None):
    arguments = list(sys.argv[1:] if argv is None else argv)
    output = DEFAULT
    try:
        raw = str(DEFAULT)
        for index, argument in enumerate(arguments):
            if argument == '--output' and index + 1 < len(arguments):
                raw = arguments[index + 1]
                break
            if argument.startswith('--output='):
                raw = argument.split('=', 1)[1]
                break
        output = owned_output(raw)
        atomic_report(output, {'status': 'INCOMPLETE', 'passed': False})
        parser = argparse.ArgumentParser(allow_abbrev=False)
        parser.add_argument('--output', default=str(DEFAULT))
        parser.add_argument('--self-test', action='store_true')
        parser.add_argument('--negative-control', choices=tuple(CODE_MAPPING))
        args = parser.parse_args(arguments)
        if owned_output(args.output) != output:
            raise ValueError('duplicate evidence output options are not accepted')
        source = capture()
        model_bytes = MODEL.read_bytes()
        digest = hashlib.sha256(model_bytes).hexdigest()
        assert digest == source['source_sha256']['scripts/formal/native_head_model.py']
        model = loaded_model(model_bytes)
        positive = model.explore(args.negative_control)
        assert positive['passed'], positive
        assert len(positive['witnesses']) == 6, positive
        for witness in positive['witnesses'].values():
            assert model.replay(witness) is None
        controls = {name: model.explore(name) for name in model.MUTANTS}
        for name, control in controls.items():
            assert not control['passed'] and control['violation'] == model.EXPECTED[name], control
            assert model.replay(control['trace'], name) == model.EXPECTED[name]
        gate_controls = failure_controls() if args.self_test else []
        assert capture() == source, 'source changed during model execution'
        report = {
            'status': 'PASS', 'passed': True,
            'scope': 'finite CAS/ownership model; documentary mapping, not refinement/provider/transport/heap proof',
            'bounds': '24 starting cases; two writer observations; floor2/current1-or3; at most2conditional PUTs; cancellation at every dispatched cut',
            'assumptions': ['linearizable provider CAS', 'immutable committed bytes/identity', 'epoch-only acquisition; roots opaque', 'source ownership excludes copied transport buffers and remote effects', 'read-allocation limits are physical-test obligations, not modeled zero reads'],
            'source': source, 'executed_model_sha256': digest, 'code_mapping': CODE_MAPPING,
            'model': positive, 'negative_controls': controls, 'failure_report_controls': gate_controls,
        }
        atomic_report(output, report)
        print(json.dumps({'passed': True, 'states': positive['states'], 'edges': positive['edges'], 'witnesses': len(positive['witnesses']), 'negative_controls': len(controls), 'failure_report_controls': len(gate_controls)}))
    except BaseException as error:
        atomic_report(output, {'status': 'FAIL', 'passed': False, 'error_type': type(error).__name__, 'error': str(error)[:4096]})
        raise

if __name__ == '__main__':
    main()
