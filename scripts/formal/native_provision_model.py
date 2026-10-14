"""Finite election model; ideal fresh distinct incarnations and immutable binding.

Not Rust correspondence or provider durability. Missing/corrupt selected heads
are observations, not authority to initialize. Registry deletion and reclaimed
unpublished candidates are outside the model. Storage and replies are separate.
"""
from collections import deque
from dataclasses import dataclass, replace, asdict

@dataclass(frozen=True)
class State:
    heads: tuple = (0, 0)  # absent, empty, advanced, missing, corrupt
    candidate: tuple = (0, 0)  # unused, pending, reply observed, ready, lost
    candidate_calls: tuple = (0, 0)
    binding: int = -1
    publication: tuple = (0, 0)  # unused, pending, observed, lost
    binding_calls: tuple = (0, 0)
    caps: tuple = (-1, -1)
    advanced: bool = False

def slot(values, i, value):
    return values[:i] + (value,) + values[i+1:]

def invariant(state):
    assert max(state.candidate_calls) <= 1, 'candidate-must-submit-once'
    assert max(state.binding_calls) <= 1, 'binding-must-submit-once'
    for cap in state.caps:
        assert cap == -1 or state.binding == cap, 'writer-must-be-stored-winner'
    if state.binding != -1:
        assert state.candidate[state.binding] == 3, 'binding-requires-ready-candidate'
        if state.advanced:
            assert state.heads[state.binding] != 1, 'known-history-must-not-reset'

def transitions(s, mutant=None):
    for i in range(2):
        if s.candidate[i] == 0 or (mutant == 'retry_candidate' and s.candidate[i] == 4 and s.candidate_calls[i] == 1):
            yield ('fresh-candidate-after-loss' if 4 in s.candidate else 'candidate-storage'), replace(s, candidate=slot(s.candidate,i,1), heads=slot(s.heads,i,1), candidate_calls=slot(s.candidate_calls,i,s.candidate_calls[i]+1))
        if s.candidate[i] == 1:
            yield 'candidate-observed', replace(s,candidate=slot(s.candidate,i,2))
            yield 'candidate-lost-reply', replace(s,candidate=slot(s.candidate,i,4))
        if s.candidate[i] == 2 and s.heads[i] == 1:
            yield 'candidate-recaptured', replace(s,candidate=slot(s.candidate,i,3))
        if (s.candidate[i] == 3 or mutant == 'unready_binding') and s.candidate[i] in (3,4) and (s.publication[i] == 0 or (mutant == 'retry_binding' and s.publication[i] == 3 and s.binding_calls[i] == 1)):
            winner = i if s.binding == -1 else s.binding
            yield ('delayed-loser-known-missing' if s.binding != -1 and s.binding != i and s.heads[s.binding] == 3 else 'binding-storage'), replace(s,binding=winner, publication=slot(s.publication,i,1), binding_calls=slot(s.binding_calls,i,s.binding_calls[i]+1))
        if s.publication[i] == 1:
            yield 'binding-observed', replace(s,publication=slot(s.publication,i,2))
            yield 'binding-lost-reply', replace(s,publication=slot(s.publication,i,3))
        if s.publication[i] in (1,2,3) and s.binding != -1 and (s.heads[s.binding] in (1,2) or mutant == 'skip_selected_validation'):
            cap = i if mutant == 'adopt_local_loser' else s.binding
            if s.caps[i] != cap:
                yield ('lost-binding-resolved' if s.publication[i] == 3 else 'validated-winner'), replace(s,caps=slot(s.caps,i,cap))
        if mutant == 'writer_before_binding' and s.candidate[i] == 3 and s.binding == -1:
            yield 'unpublished-writer', replace(s,caps=slot(s.caps,i,i))
    if s.binding != -1:
        winner = s.binding
        if winner in s.caps and s.heads[winner] == 1:
            yield 'selected-append', replace(s,heads=slot(s.heads,winner,2),advanced=True)
        if s.heads[winner] in (1,2):
            yield 'known-missing', replace(s,heads=slot(s.heads,winner,3))
            yield 'known-corrupt', replace(s,heads=slot(s.heads,winner,4))
        if mutant in ('reset_known_missing', 'reset_known_empty') and s.heads[winner] == 3 and (s.advanced or mutant == 'reset_known_empty'):
            yield 'unsafe-empty-reset', replace(s,heads=slot(s.heads,winner,1))

def transition_oracle(before, action, after):
    for old, new in zip(before.caps, after.caps):
        if new != old and new != -1:
            assert before.binding == new and before.heads[new] in (1,2), 'new-capability-requires-valid-selected-head'
    if before.binding != -1:
        assert after.binding == before.binding, 'published-selection-must-not-change'
        if before.heads[before.binding] in (3,4):
            assert after.heads[before.binding] == before.heads[before.binding], 'known-missing-or-corrupt-must-not-reset'

def run(mutant=None):
    start=State(); seen={start}; queue=deque([start]); witnesses={}; edges=0
    while queue:
        state=queue.popleft(); invariant(state)
        for action, after in transitions(state,mutant):
            transition_oracle(state, action, after); invariant(after); edges+=1; witnesses.setdefault(action, {'steps':[asdict(state),asdict(after)]})
            if after not in seen: seen.add(after); queue.append(after)
    return {'states':len(seen),'edges':edges,'witnesses':sorted(witnesses),'completed_witnesses':witnesses}

EXPECTED={
    'retry_candidate':'candidate-must-submit-once',
    'retry_binding':'binding-must-submit-once',
    'adopt_local_loser':'new-capability-requires-valid-selected-head',
    'writer_before_binding':'new-capability-requires-valid-selected-head',
    'unready_binding':'binding-requires-ready-candidate',
    'reset_known_missing':'known-missing-or-corrupt-must-not-reset',
    'reset_known_empty':'known-missing-or-corrupt-must-not-reset',
    'skip_selected_validation':'new-capability-requires-valid-selected-head',
}
if __name__ == '__main__':
    import json
    positive=run(); killed=[]
    for name,oracle in EXPECTED.items():
        try: run(name)
        except AssertionError as error:
            assert str(error)==oracle,(name,str(error));killed.append(name)
        else: raise AssertionError('escaped mutant: '+name)
    print(json.dumps({'passed':True,**positive,'mutants_killed':killed}))
