"""Finite head CAS/ownership transitions and independent safety oracle."""
from collections import deque
from dataclasses import asdict,dataclass,replace
from itertools import product
ROOTS=(23,29)
MUTANTS=("scope","budget","root_preservation","ambiguous_retry","physical_cancel","source_lifetime","notify_order","conflict_floor")
EXPECTED={"scope":"wrong-provider-key-touched-conditional-write","budget":"oversize-input-touched-conditional-write","root_preservation":"epoch-acquire-changed-root","ambiguous_retry":"ambiguous-publication-retried-or-reclassified","physical_cancel":"started-operation-outlived-physical-owner","source_lifetime":"source-payload-outlived-physical-owner","notify_order":"drain-notified-before-owned-resources-terminal","conflict_floor":"acquired-fence-below-observed-floor"}
@dataclass(frozen=True)
class State:
    phase: str = "admit"
    epoch: int = 2
    roots: tuple = ROOTS
    scope: bool = True
    fits: bool = True
    conflict: str = "none"
    lost_reply: bool = False
    floor: int = 2
    attempts: int = 0
    writes: int = 0
    unknown: bool = False
    unknown_attempts: int = 0
    cancelled: bool = False
    source: bool = False
    physical: bool = False
    permit: bool = False
    notified: bool = False
    outcome: str = "pending"
    written_epoch: int = 0

def successors(state, mutant):
    """Two racing head observations and one owned write, with cancel at any cut."""
    s = state
    if s.phase == "admit":
        if (not s.scope and mutant != "scope") or (not s.fits and mutant != "budget"):
            yield "reject-before-io", replace(s, phase="done", outcome="error")
        else:
            yield "dispatch-owned", replace(s, phase="ready", physical=True, permit=True)
    elif s.phase == "ready":
        if s.cancelled:
            yield "cancel-before-write", replace(s, phase="cleanup", outcome="cancelled")
        else:
            yield "start-conditional-put", replace(s, phase="io", source=True, attempts=s.attempts + 1)
    elif s.phase == "io":
        if s.conflict != "none":
            # A competing writer changes the provider version after the read.
            current = 3 if s.conflict == "higher" else 1
            if current < s.floor and mutant != "conflict_floor":
                yield "reject-regressed-conflict", replace(s, phase="cleanup", source=False, outcome="error")
            else:
                yield "bounded-conflict-reread", replace(s, phase="ready", source=False, epoch=current, conflict="none")
        else:
            roots = (31, 37) if mutant == "root_preservation" else s.roots
            epoch = s.epoch + 1
            yield "publish-head-cas", replace(s, phase="response", epoch=epoch, roots=roots, written_epoch=epoch, writes=s.writes + 1)
    elif s.phase == "response":
        if s.lost_reply:
            yield "lost-reply-unknown", replace(s, phase="unknown", unknown=True, unknown_attempts=s.attempts, outcome="unknown")
        else:
            yield "confirmed-stored", replace(s, phase="cleanup", source=False, outcome="stored")
    elif s.phase == "unknown":
        if mutant == "ambiguous_retry" and s.attempts < 2:
            yield "incorrect-retry-after-unknown", replace(s, phase="ready", source=False, lost_reply=False)
        else:
            yield "terminal-unknown", replace(s, phase="cleanup", source=False)
    elif s.phase == "cleanup":
        if s.source:
            yield "release-source", replace(s, source=False)
        elif s.permit:
            if mutant == "notify_order":
                yield "incorrect-notify-before-permit", replace(s, physical=False, notified=True)
            else:
                yield "release-permit", replace(s, permit=False)
        else:
            yield "notify-physical-terminal", replace(s, phase="done", physical=False, notified=True)
    if s.phase in {"ready", "io", "response", "unknown"} and not s.cancelled:
        updated = replace(s, cancelled=True)
        if mutant == "physical_cancel":
            updated = replace(updated, physical=False, permit=False, notified=True)
        yield "caller-or-runtime-cancel", updated
    if s.phase in {"response", "unknown"} and mutant == "source_lifetime" and s.source:
        yield "incorrect-wrapper-release-before-source", replace(s, phase="cleanup", physical=False, permit=False, notified=True)

def violation(state):
    """Independent observable safety oracle; does not call transition predicates."""
    s = state
    if s.attempts and not s.scope:
        return "wrong-provider-key-touched-conditional-write"
    if s.attempts and not s.fits:
        return "oversize-input-touched-conditional-write"
    if s.writes and s.roots != ROOTS:
        return "epoch-acquire-changed-root"
    if s.writes and s.written_epoch <= s.floor:
        return "acquired-fence-below-observed-floor"
    if s.unknown and (s.attempts > s.unknown_attempts or s.outcome == "stored"):
        return "ambiguous-publication-retried-or-reclassified"
    if s.phase in {"io", "response", "unknown"} and not s.physical:
        return "started-operation-outlived-physical-owner"
    if s.source and not s.physical:
        return "source-payload-outlived-physical-owner"
    if s.notified and (s.permit or s.source or s.physical):
        return "drain-notified-before-owned-resources-terminal"
    return None

def explore(mutant=None):
    queue = deque()
    for scope, fits, conflict, lost in product((False, True), (False, True), ("none", "higher", "regressed"), (False, True)):
        initial=State(scope=scope, fits=fits, conflict=conflict, lost_reply=lost)
        queue.append((initial,initial,()))
    seen, edges, witnesses = set(), 0, {}
    while queue:
        state, initial, trace = queue.popleft()
        if state in seen:
            continue
        seen.add(state)
        error = violation(state)
        if error:
            return {"passed": False, "violation": error, "trace": trace_record(initial,trace,state), "states": len(seen), "edges": edges}
        if state.phase == "done":
            if state.writes and state.unknown:
                witnesses.setdefault("stored-write-lost-reply-unknown", trace_record(initial,trace,state))
            if state.outcome == "stored" and state.attempts == 2:
                witnesses.setdefault("competing-fence-reread-and-acquire", trace_record(initial,trace,state))
            if state.cancelled and state.writes:
                witnesses.setdefault("caller-cancelled-write-physically-completed", trace_record(initial,trace,state))
            if state.scope and not state.fits and state.attempts == 0:
                witnesses.setdefault("oversize-no-conditional-write", trace_record(initial,trace,state))
            if not state.scope and state.fits and state.attempts == 0:
                witnesses.setdefault("wrong-scope-no-conditional-write", trace_record(initial,trace,state))
            if state.scope and state.fits and state.attempts==1 and state.conflict=="regressed" and state.outcome=="error" and "reject-regressed-conflict" in trace:
                witnesses.setdefault("regressed-conflict-terminal", trace_record(initial,trace,state))
        for label, updated in successors(state, mutant):
            edges += 1
            queue.append((updated, initial, trace + (label,)))
    return {"passed": True, "states": len(seen), "edges": edges, "witnesses": witnesses}


def trace_record(initial,actions,terminal):
    return {"initial":asdict(initial),"actions":list(actions),"terminal":asdict(terminal)}

def state_from_record(record):
    values=dict(record);values['roots']=tuple(values['roots'])
    return State(**values)

def replay(record,mutant=None):
    state=state_from_record(record['initial'])
    assert state.phase=='admit' and state.epoch==2 and state.roots==ROOTS
    for label in record['actions']:
        assert violation(state) is None, 'trace continues after a safety violation'
        matching=[next_state for event,next_state in successors(state,mutant) if event==label]
        assert len(matching)==1, (state,label)
        state=matching[0]
    assert state==state_from_record(record['terminal']), 'replay terminal mismatch'
    return violation(state)
