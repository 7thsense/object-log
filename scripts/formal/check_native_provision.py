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


MODEL = ROOT / 'scripts/formal/native_provision_model.py'
DEFAULT = ROOT / 'target/native-provision-model.json'

def execute(raw):
    module = types.ModuleType('captured_native_provision_model')
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
        assert source['source_sha256']['scripts/formal/native_provision_model.py'] == digest
        model = execute(raw)
        if args.negative_control and args.negative_control not in model.EXPECTED: raise ValueError('unknown model control')
        positive = model.run(args.negative_control)
        required = {'binding-lost-reply','binding-observed','binding-storage',
            'candidate-lost-reply','candidate-observed','candidate-recaptured','candidate-storage',
            'known-corrupt','known-missing','selected-append','validated-winner',
            'fresh-candidate-after-loss','delayed-loser-known-missing','lost-binding-resolved'}
        assert positive['states'] == 673 and positive['edges'] == 1658
        assert set(positive['witnesses']) == required
        assert set(positive['completed_witnesses']) == required
        assert all(case['steps'] for case in positive['completed_witnesses'].values())
        required_controls = {'retry_candidate','retry_binding','adopt_local_loser',
            'writer_before_binding','unready_binding','reset_known_missing','reset_known_empty','skip_selected_validation'}
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
            with tempfile.TemporaryDirectory(dir=DEFAULT.parent, prefix='provision-gate-') as directory:
                test_output=Path(directory)/'report.json'
                for extra in [['--negative-control', 'retry_candidate'], ['--invalid-option'], ['--negative-control','unknown']]:
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
                isolated_script = isolated/'scripts/formal/check_native_provision.py'
                isolated_report = isolated/'target/report.json'
                for filename, broken in [('check_native_head.py',None),('check_native_head.py',b'invalid python!'),
                    ('native_provision_model.py',None),('native_provision_model.py',b'invalid python!')]:
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
        atomic_report(output, {'status':'PASS','passed':True,'scope':'finite abstract candidate election model; documentary source binding, not executed Rust correspondence or provider/resource/frame proof',
            'bounds':'two fresh candidates; one immutable binding; separate storage and reply observations; empty/advanced/missing/corrupt candidate heads',
            'assumptions':['distinct fresh incarnations','linearizable absent-token CAS','immutable published binding','unreachable unpublished candidates retained','registry deletion not modeled'],
            'source':source,'executed_model_sha256':digest,'executed_helper_sha256':hashlib.sha256(helper_bytes).hexdigest(),'model':positive,'negative_controls':controls,'failure_report_controls':gate_controls})
        print(json.dumps({'passed':True,'states':positive['states'],'witnesses':len(positive['witnesses']),'negative_controls':len(controls),'failure_report_controls':len(gate_controls)}))
    except BaseException as error:
        atomic_report(output, {'status':'FAIL','passed':False,'error_type':type(error).__name__,'error':str(error)[:4096]})
        raise

if __name__ == '__main__': main()
