#!/usr/bin/env python3
"""Failure-safe captured execution of the bounded COW abstraction."""
import argparse
import hashlib
import json
import subprocess
import sys
import tempfile
import shutil
import types
from pathlib import Path
import os
ROOT = Path(__file__).resolve().parents[2]

def owned_output(raw):
    path = Path(raw)
    if not path.is_absolute(): path = ROOT / path
    target = ROOT / 'target'
    if not path.is_relative_to(target) or path.suffix != '.json': raise ValueError('output must be target-owned JSON')
    current = ROOT
    for part in path.relative_to(ROOT).parts:
        if part in {'.','..'}: raise ValueError('output traversal')
        current /= part
        if current.is_symlink(): raise ValueError('output symlink')
    if not path.resolve().is_relative_to(target.resolve()): raise ValueError('output escape')
    return path

def atomic_report(path, value):
    owned_output(str(path))
    path.parent.mkdir(parents=True, exist_ok=True)
    owned_output(str(path))
    descriptor, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name+'.')
    try:
        with os.fdopen(descriptor,'w') as output:
            json.dump(value,output,indent=2); output.write('\n'); output.flush(); os.fsync(output.fileno())
        os.replace(temporary,path)
    finally:
        if os.path.exists(temporary): os.unlink(temporary)


MODEL = ROOT / 'scripts/formal/native_cow_model.py'
DEFAULT = ROOT / 'target/native-cow-model.json'

def execute(raw):
    module = types.ModuleType('captured_native_cow_model')
    sys.modules[module.__name__] = module
    exec(compile(raw, str(MODEL), 'exec'), module.__dict__)
    return module

def main():
    arguments = sys.argv[1:]
    raw_output = str(DEFAULT)
    for index, argument in enumerate(arguments):
        if argument == '--output' and index+1 < len(arguments): raw_output=arguments[index+1]; break
        if argument.startswith('--output='): raw_output=argument.split('=',1)[1]; break
    output = owned_output(raw_output)
    atomic_report(output, {'status':'INCOMPLETE','passed':False})
    try:
        parser = argparse.ArgumentParser(allow_abbrev=False)
        parser.add_argument('--output', default=str(DEFAULT))
        parser.add_argument('--self-test', action='store_true')
        parser.add_argument('--negative-control')
        args = parser.parse_args(arguments)
        if owned_output(args.output) != output: raise ValueError('duplicate output options')
        helper_path = ROOT / 'scripts/formal/check_native_head.py'
        helper_bytes = helper_path.read_bytes()
        helper = types.ModuleType('captured_native_head_gate_helper')
        helper.__file__ = str(helper_path)
        exec(compile(helper_bytes, str(helper_path), 'exec'), helper.__dict__)
        capture = helper.capture
        source = capture()
        assert source['source_sha256']['scripts/formal/check_native_head.py'] == hashlib.sha256(helper_bytes).hexdigest()
        raw = MODEL.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        assert source['source_sha256']['scripts/formal/native_cow_model.py'] == digest
        model = execute(raw)
        if args.negative_control and args.negative_control not in model.EXPECTED: raise ValueError('unknown model control')
        positive = model.run(args.negative_control)
        required = {'complete-competitor-rejected','height-0','height-1','height-2','intermediate-split-parent-room',
            'lost-not-stored-unchanged','lost-stored-later-append','missing-or-corrupt-unknown',
            'ninth-explicit-rejection','nonempty-prefix','old-root-value-preserved',
            'partial-occupancy-unknown','rewritten-prefix-unknown','same-epoch-stale-token',
            'scope-regression-unknown','storage-before-lost-reply','submitted-once',
            'group-all-or-none','group-proof-budget','group-later-append-resolution',
            'group-merged-and-split','group-mismatch-before-incomplete',
            'group-nonempty-prefix-rewrite','group-matching-first-wrong-last'}
        assert positive['checked_appends'] == 3586 and positive['retained_roots'] == 510
        assert set(positive['witnesses']) == required
        assert set(positive['completed_witnesses']) == required
        assert all(case['steps'] for case in positive['completed_witnesses'].values())
        required_controls = {'prefix_skip','nonlast_skip','numeric_head','certificate_base',
            'absence_skip','resubmit','drop_split','reverse_growth','group_early_reject','group_prefix_skip','group_first_only'}
        assert set(model.EXPECTED) == required_controls, 'model control coverage changed'
        controls = {}
        for name, expected in model.EXPECTED.items():
            try: model.run(name)
            except AssertionError as error:
                assert str(error) == expected, (name,str(error))
                controls[name]={'killed':True,'oracle':expected}
            else: raise AssertionError('escaped model mutant: '+name)
        gate_controls=[]
        if args.self_test:
            with tempfile.TemporaryDirectory(dir=DEFAULT.parent, prefix='cow-gate-') as directory:
                test_output=Path(directory)/'report.json'
                for extra in [['--negative-control', 'prefix_skip'], ['--invalid-option'], ['--negative-control','unknown']]:
                    previous=subprocess.run([sys.executable,__file__,'--output',str(test_output)],capture_output=True,text=True)
                    assert previous.returncode == 0 and json.loads(test_output.read_text())['status']=='PASS'
                    result=subprocess.run([sys.executable,__file__,'--output',str(test_output),*extra],capture_output=True,text=True)
                    failed=json.loads(test_output.read_text())
                    assert result.returncode != 0 and failed['status']=='FAIL' and not failed['passed']
                    gate_controls.append(extra)
                isolated = Path(directory)/'workspace'
                for name in source['source_sha256']:
                    target = isolated/name
                    target.parent.mkdir(parents=True,exist_ok=True)
                    shutil.copy2(ROOT/name,target)
                isolated_script = isolated/'scripts/formal/check_native_cow.py'
                isolated_report = isolated/'target/report.json'
                for filename, broken in [('check_native_head.py',None),('check_native_head.py',b'invalid python!'),
                    ('native_cow_model.py',None),('native_cow_model.py',b'invalid python!')]:
                    changed = isolated/'scripts/formal'/filename
                    original = changed.read_bytes()
                    command = [sys.executable,str(isolated_script),'--output',str(isolated_report)]
                    previous = subprocess.run(command,capture_output=True,text=True)
                    assert previous.returncode == 0 and json.loads(isolated_report.read_text())['status']=='PASS'
                    try:
                        if broken is None: changed.unlink()
                        else: changed.write_bytes(broken)
                        failed = subprocess.run(command,capture_output=True,text=True)
                        report = json.loads(isolated_report.read_text())
                        assert failed.returncode != 0 and report['status']=='FAIL' and not report['passed']
                        gate_controls.append([filename,'missing' if broken is None else 'syntax'])
                    finally: changed.write_bytes(original)
        assert capture() == source, 'source changed during captured execution'
        atomic_report(output, {'status':'PASS','passed':True,'scope':'finite abstract COW/CAS/content-resolution model; documentary source binding, not executed Rust correspondence or provider/resource/frame proof',
            'bounds':'leaf2/fanout2/height2; exhaustive two-symbol sequences through eight entries; explicit ninth rejection; seeded intermediate split; abstract certificate identity/count/start/payload',
            'assumptions':['injective immutable page commitments','linearizable exact-token CAS','physical availability of retained pages','unreadable observations abstract missing/corrupt touched data'],
            'source':source,'executed_model_sha256':digest,'executed_helper_sha256':hashlib.sha256(helper_bytes).hexdigest(),'model':positive,'negative_controls':controls,'failure_report_controls':gate_controls})
        print(json.dumps({'passed':True,'checked_appends':positive['checked_appends'],'witnesses':len(positive['witnesses']),'negative_controls':len(controls),'failure_report_controls':len(gate_controls)}))
    except BaseException as error:
        atomic_report(output, {'status':'FAIL','passed':False,'error_type':type(error).__name__,'error':str(error)[:4096]})
        raise

if __name__ == '__main__': main()
