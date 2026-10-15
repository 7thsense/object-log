#!/usr/bin/env python3
"""Failure-safe captured execution of the bounded coupled protocol abstraction."""
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


MODEL = ROOT / 'scripts/formal/native_semantic_model.py'
DEFAULT = ROOT / 'target/native-semantic-model.json'
REQUIRED = {
    'seal_pair_skip':'seal/index binding', 'zero_append_rewrite':'same-offset exact roots',
    'ancestry_skip':'exact manifest ancestry', 'ignore_budget':'proof exact budget',
    'wrong_transition':'assigned transition fields', 'wrong_map':'atomic ghost map', 'split_publish':'atomic ghost log',
    'cas_token_skip':'stored epoch monotonicity', 'stale_cut_skip':'stale cut before writes',
    'bootstrap_nonempty':'nonempty bootstrap refused', 'resubmit':'single original submission',
    'reset_fence':'sticky unreadable fence', 'publish_floor_skip':'known floor forbids CAS', 'forget_floor':'resolution retains authority',
    'numeric_durable':'durable original coupled content', 'first_only':'durable original coupled content',
    'early_reject':'complete incompatible rejection', 'scan_latest':'scan original captured contents',
    'scan_eof':'scan verified completion', 'scan_skip_token':'scan continuation integrity',
    'resolve_budget_skip':'durable original coupled content', 'resolve_missing_skip':'durable original coupled content',
    'resolve_unsubmitted':'unsubmitted resolve refused',
    'resolve_candidate_skip':'durable original coupled content', 'durable_floor_reset':'resolution retains authority',
}
WITNESSES = {'seal-pairing','same-offset-and-ancestry','logarithmic-proof','durable-before-reply-loss',
    'lost-reply-later-resolution','stale-cut-and-one-shot','bootstrap','sticky-fence','retained-floor-unknown',
    'retained-floor-rejected','partial-mismatch','captured-scan','noop-and-empty','fence-before-publication',
    'exhaustive-competing-groups','scan-byte-work-and-exact-limits','resolution-source-and-budget'}

def ensure(condition, message):
    if not condition: raise AssertionError(message)

def execute(raw):
    module = types.ModuleType('captured_native_semantic_model')
    sys.modules[module.__name__] = module
    exec(compile(raw, str(MODEL), 'exec'), module.__dict__)
    return module

def self_controls(source):
    controls=[]
    with tempfile.TemporaryDirectory(dir=DEFAULT.parent,prefix='semantic-gate-') as directory:
        test_output=Path(directory)/'report.json'
        command=[sys.executable,__file__,'--output',str(test_output)]
        for extra in [['--negative-control','seal_pair_skip'],['--invalid-option'],
                      ['--negative-control','unknown'],['--max-pairs','1']]:
            prior=subprocess.run(command,capture_output=True,text=True)
            ensure(prior.returncode==0 and json.loads(test_output.read_text())['status']=='PASS','positive gate precondition')
            failed=subprocess.run(command+extra,capture_output=True,text=True)
            report=json.loads(test_output.read_text())
            ensure(failed.returncode!=0 and report['status']=='FAIL' and not report['passed'],'stale PASS survived failing invocation')
            controls.append(extra)
        isolated=Path(directory)/'workspace'
        for name in source['source_sha256']:
            target=isolated/name
            target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copy2(ROOT/name,target)
        command=[sys.executable,str(isolated/'scripts/formal/check_native_semantic.py'),'--output',str(isolated/'target/report.json')]
        for filename,broken in [('check_native_head.py',None),('check_native_head.py',b'invalid python!'),
                                ('native_semantic_model.py',None),('native_semantic_model.py',b'invalid python!')]:
            changed=isolated/'scripts/formal'/filename
            original=changed.read_bytes()
            prior=subprocess.run(command,capture_output=True,text=True)
            ensure(prior.returncode==0,'isolated positive precondition')
            try:
                if broken is None: changed.unlink()
                else: changed.write_bytes(broken)
                failed=subprocess.run(command,capture_output=True,text=True)
                report=json.loads((isolated/'target/report.json').read_text())
                ensure(failed.returncode!=0 and report['status']=='FAIL' and not report['passed'],'broken source/helper retained PASS')
                controls.append([filename,'missing' if broken is None else 'syntax'])
            finally: changed.write_bytes(original)
    return controls

def main():
    arguments=sys.argv[1:]
    raw_output=str(DEFAULT)
    for index,argument in enumerate(arguments):
        if argument=='--output' and index+1<len(arguments): raw_output=arguments[index+1];break
        if argument.startswith('--output='): raw_output=argument.split('=',1)[1];break
    output=owned_output(raw_output)
    atomic_report(output,{'status':'INCOMPLETE','passed':False})
    try:
        parser=argparse.ArgumentParser(allow_abbrev=False)
        parser.add_argument('--output',default=str(DEFAULT))
        parser.add_argument('--self-test',action='store_true')
        parser.add_argument('--negative-control')
        parser.add_argument('--max-pairs',type=int,default=8000)
        args=parser.parse_args(arguments)
        ensure(owned_output(args.output)==output,'duplicate output options')
        ensure(args.max_pairs>0,'nonpositive exploration bound')
        helper_path=ROOT/'scripts/formal/check_native_head.py'
        helper_bytes=helper_path.read_bytes()
        helper=types.ModuleType('captured_semantic_source_helper')
        helper.__file__=str(helper_path)
        exec(compile(helper_bytes,str(helper_path),'exec'),helper.__dict__)
        source=helper.capture()
        ensure(source['source_sha256']['scripts/formal/check_native_head.py']==hashlib.sha256(helper_bytes).hexdigest(),'helper capture mismatch')
        raw=MODEL.read_bytes()
        digest=hashlib.sha256(raw).hexdigest()
        ensure(source['source_sha256']['scripts/formal/native_semantic_model.py']==digest,'model capture mismatch')
        model=execute(raw)
        ensure(model.EXPECTED==REQUIRED,'required omission oracles changed')
        if args.negative_control is not None: ensure(args.negative_control in REQUIRED,'unknown control')
        positive=model.run(args.negative_control,args.max_pairs)
        ensure(positive['checked_pairs']==8000 and positive['seeded_generations']==9,'incomplete finite exploration')
        ensure(set(positive['witnesses'])==WITNESSES,'witness coverage changed')
        ensure(all(w['completed'] and w['steps'] for w in positive['witnesses'].values()),'incomplete witness')
        controls={}
        for name,expected in REQUIRED.items():
            observed=[]
            for _ in range(2):
                try: model.run(name,args.max_pairs)
                except model.OracleFailure as failure:
                    ensure(str(failure)==expected and bool(failure.trace),'wrong or empty omission witness: '+name)
                    observed.append((str(failure),failure.trace))
                else: raise AssertionError('escaped omission: '+name)
            ensure(observed[0]==observed[1],'omission witness cannot reproduce: '+name)
            controls[name]={'killed':True,'oracle':expected,'steps':list(observed[0][1]),'deterministic_rerun':True}
        gate=self_controls(source) if args.self_test else []
        ensure(helper.capture()==source,'source changed during captured execution')
        atomic_report(output,{'status':'PASS','passed':True,
            'scope':'independent bounded coupled-publication protocol abstraction; documentary source capture, not Rust refinement, COW/wire/provider/residency or release qualification',
            'assumptions':['injective immutable commitments','canonical private map/manifest/frame/index/seal construction','linearizable exact-original-token CAS','binding abstracts source/namespace/literal authority identity','touched-source failures abstracted as unavailable'],
            'source':source,'executed_model_sha256':digest,'executed_helper_sha256':hashlib.sha256(helper_bytes).hexdigest(),
            'model':positive,'negative_controls':controls,'failure_report_controls':gate})
        print(json.dumps({'passed':True,'checked_pairs':positive['checked_pairs'],'witnesses':len(WITNESSES),'negative_controls':len(controls),'failure_report_controls':len(gate)}))
    except BaseException as error:
        atomic_report(output,{'status':'FAIL','passed':False,'error_type':type(error).__name__,'error':str(error)[:4096]})
        raise

if __name__=='__main__': main()
