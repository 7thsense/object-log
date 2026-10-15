"""Independent finite coupled-publication protocol model, NOT Rust refinement.

Three ordered abstract keys; two nonempty values plus empty; two payloads; one/two-entry groups with
one/two records per entry; exhaustive competing group choices and explicit
nine-generation skip history. Ideal injective immutable commitments, canonical
private construction and linearizable exact-token CAS are assumptions. Flat
index/map values do not model Rust COW shape, wire bytes, providers or residency.
Storage commit, reply loss, observed authority and local knowledge are separate.
"""
from dataclasses import dataclass, replace
from itertools import product

class Unavailable(Exception): pass
class Stale(Exception): pass
class OutputLimit(Exception): pass
class ExplorationLimit(Exception): pass
class OracleFailure(AssertionError):
    def __init__(self, oracle, trace):
        super().__init__(oracle)
        self.trace = tuple(trace)

def require(condition, oracle, *trace):
    if not condition: raise OracleFailure(oracle, trace)

def flat_edit(state, edits):
    result = dict(state)
    for key, value in edits:
        if value is None: result.pop(key, None)
        else: result[key] = value
    return tuple(sorted(result.items()))

def ghost_apply(snapshot, changes):
    # Independent oracle: select the last declaration for each key, rather than
    # mutating the implementation's dictionary or calling its edit fold.
    keys = sorted({key for key, _ in snapshot} | {key for key, _ in changes})
    result = []
    for key in keys:
        declarations = [value for candidate, value in changes if candidate == key]
        original = [value for candidate, value in snapshot if candidate == key]
        chosen = declarations[-1:] if declarations else original
        if chosen and chosen[0] is not None:
            result.append((key, chosen[0]))
    return tuple(result)

@dataclass(frozen=True)
class Manifest:
    generation: int
    through: int
    start: int
    count: int
    state: tuple
    links: tuple
@dataclass(frozen=True)
class Frame:
    identity: int
    payload: int
    base: int
    count: int
    before: int | None
    after: int
    generation: int
    start: int
    end: int
    entries: int
    ordinal: int
@dataclass(frozen=True)
class Index:
    frames: tuple = ()
    layout: int = 0
    @property
    def end(self): return self.frames[-1].base + self.frames[-1].count if self.frames else 0
@dataclass(frozen=True)
class Seal:
    manifest: int
    index: Index
    through: int
@dataclass(frozen=True)
class Head:
    token: int = 0
    epoch: int = 0
    index: Index = Index()
    seal: Seal | None = None
    binding: tuple = (1,1,1)
    readable: bool = True
@dataclass
class Knowledge:
    floor: Head
    highest_epoch: int = 0
@dataclass
class Writer:
    original: Head
    knowledge: Knowledge
@dataclass
class Attempt:
    original: Head
    candidate: Head
    inputs: tuple
    edits: tuple
    frames: tuple
    expected_map: tuple
    knowledge: Knowledge
    attempted: bool = False
    calls: int = 0
@dataclass(frozen=True)
class GhostGroup:
    start: int
    end: int
    inputs: tuple
    before: tuple
    after: tuple
@dataclass(frozen=True)
class Token:
    cut: Head
    ghost: tuple
    lower: int = 0
    upper: int = 2
    after: int = -1
@dataclass(frozen=True)
class Page:
    rows: tuple
    complete: bool
    token: Token | None

class Budget:
    def __init__(self, count, mutant=None): self.left, self.used, self.mutant = count, 0, mutant
    def read(self):
        if self.left <= 0 and self.mutant != 'ignore_budget': raise Unavailable('proof budget')
        self.left -= 1
        self.used += 1

class Machine:
    def __init__(self, mutant=None):
        self.mutant = mutant
        self.objects = {}
        self.read_calls = 0
        self.intern = {}
        self.missing = set()
        self.missing_scan = set()
        self.current = Head()
        self.ghost_map = ()
        self.ghost_log = ()
        self.ghost_groups = ()
        self.ghost_snapshots = {self.current: ()}
        self.ghost_manifests = {}
    def put(self, value):
        if value not in self.intern:
            reference = len(self.objects) + 1
            self.intern[value] = reference
            self.objects[reference] = value
        return self.intern[value]
    def read(self, reference, budget, generation=None, through=None):
        self.read_calls += 1
        budget.read()
        if (reference in self.missing and self.mutant != 'resolve_missing_skip') or reference not in self.objects: raise Unavailable('manifest missing/corrupt')
        value = self.objects[reference]
        if generation is not None and value.generation != generation: raise Unavailable('generation')
        if through is not None and value.through != through: raise Unavailable('through')
        return value
    def state(self, head):
        if head.seal is None: return ()
        return self.objects[head.seal.manifest].state
    def ancestry(self, before, after, budget):
        if self.mutant == 'ancestry_skip': return True
        target = self.objects[before]
        current = self.objects[after]
        if target.generation > current.generation or target.through > current.through: return False
        reference = after
        generation, through = current.generation, current.through
        while True:
            current = self.read(reference, budget, generation, through)
            if generation == target.generation: return reference == before and through == target.through
            level = (generation - target.generation).bit_length() - 1
            reference, through = current.links[level]
            generation -= 1 << level
    def origin(self, reference, budget):
        value = self.objects[reference]
        generation, through = value.generation, value.through
        while True:
            value = self.read(reference, budget, generation, through)
            if generation == 1: return value.start == 0 and not value.links
            level = (generation - 1).bit_length() - 1
            reference, through = value.links[level]
            generation -= 1 << level
    def valid(self, head, budget):
        if not head.readable: raise Unavailable('root unreadable')
        if head.seal is None: return True
        if head.seal.through != head.index.end: return False
        if self.mutant != 'seal_pair_skip' and head.seal.index != head.index: return False
        value = self.read(head.seal.manifest, budget, through=head.index.end)
        return value.through > 0 and value.generation > 0 and value.generation <= value.through
    def successor(self, before, after, budget):
        if before.binding != after.binding or after.epoch < before.epoch or after.index.end < before.index.end: return False
        if not self.valid(after, budget): return False
        if (self.mutant != 'zero_append_rewrite' and before.index.end == after.index.end
            and (before.index != after.index or before.seal != after.seal)): return False
        if after.index.frames[:len(before.index.frames)] != before.index.frames: return False
        if before.seal is None:
            if after.seal is None: return True
            return not before.index.frames and self.origin(after.seal.manifest, budget)
        if after.seal is None: return False
        return self.ancestry(before.seal.manifest, after.seal.manifest, budget)
    def observe(self, knowledge, head, budget):
        if head.binding != knowledge.floor.binding: return False
        if self.mutant != 'reset_fence': knowledge.highest_epoch = max(knowledge.highest_epoch, head.epoch)
        if head.epoch < knowledge.highest_epoch: return False
        if not self.successor(knowledge.floor, head, budget): return False
        if self.mutant == 'reset_fence': knowledge.highest_epoch = max(knowledge.highest_epoch, head.epoch)
        knowledge.floor = head
        return True
    def prepare(self, writer, expected, inputs, edits):
        if self.mutant != 'stale_cut_skip' and expected != writer.original: raise Stale('expected cut')
        original = writer.original
        if writer.knowledge.floor != original or writer.knowledge.highest_epoch > original.epoch: raise Stale('known floor/fence')
        if original.seal is None and original.index.frames and self.mutant != 'bootstrap_nonempty': raise Stale('bootstrap')
        assert inputs and all(1 <= count <= 2 for _, _, count in inputs)
        assert all(left[0] < right[0] for left, right in zip(edits, edits[1:]))
        previous = original.seal.manifest if original.seal else None
        old = self.state(original)
        generation = self.objects[previous].generation + 1 if previous else 1
        start = original.index.end
        end = start + sum(count for _, _, count in inputs)
        state = old if self.mutant == 'wrong_map' and len(edits)==1 else flat_edit(old, edits)
        links = []
        if previous:
            links.append((previous, self.objects[previous].through))
            for level in range(1, (generation - 1).bit_length()):
                middle = self.objects[links[level-1][0]]
                links.append(middle.links[level-1])
        manifest = self.put(Manifest(generation, end, start, len(inputs), state, tuple(links)))
        frames, base = [], start
        for ordinal, (identity, payload, count) in enumerate(inputs):
            frames.append(Frame(identity, payload, base, count, previous, manifest, generation, start, end, len(inputs),
                                ordinal+1 if self.mutant == 'wrong_transition' else ordinal))
            base += count
        index = Index(original.index.frames + tuple(frames))
        candidate = Head(original.token+1, original.epoch, index, Seal(manifest, index, end), original.binding)
        return Attempt(original, candidate, inputs, edits, tuple(frames),
                       ghost_apply(self.ghost_snapshots[original], edits), writer.knowledge)
    def check_committed(self):
        require(self.state(self.current)==self.ghost_map,'atomic ghost map','durable source','independent ghost map')
        actual_log=tuple((frame.identity,frame.payload,frame.count) for frame in self.current.index.frames)
        require(actual_log==self.ghost_log,'atomic ghost log','durable source','independent ghost log')
        cursor = 0
        physical = 0
        for generation, group in enumerate(self.ghost_groups, 1):
            require(group.start == cursor and group.end == cursor + sum(count for _,_,count in group.inputs),
                    'assigned transition fields','independent ghost offsets')
            manifest_ref = self.ghost_manifests[generation]
            manifest = self.objects[manifest_ref]
            require((manifest.generation,manifest.start,manifest.through,manifest.count,manifest.state) ==
                    (generation,group.start,group.end,len(group.inputs),group.after),
                    'assigned transition fields','independent manifest fields')
            for ordinal, (identity,payload,count) in enumerate(group.inputs):
                frame = self.current.index.frames[physical]
                require((frame.identity,frame.payload,frame.base,frame.count,frame.before,frame.after,
                         frame.generation,frame.start,frame.end,frame.entries,frame.ordinal) ==
                        (identity,payload,cursor,count,self.ghost_manifests.get(generation-1),manifest_ref,
                         generation,group.start,group.end,len(group.inputs),ordinal),
                        'assigned transition fields','independent submitted group',str(generation),str(ordinal))
                cursor += count
                physical += 1
        require(self.current.seal is None or (self.current.seal.index==self.current.index and self.current.seal.through==self.current.index.end),
                'atomic coupled roots','durable source','complete index and seal')
    def fence(self, original):
        if self.current.token!=original.token or self.current.binding!=original.binding:return 'conflict'
        prior=self.current
        self.current=replace(prior,token=prior.token+1,epoch=prior.epoch+1)
        require(self.current.index==prior.index and self.current.seal==prior.seal,'same-offset exact roots','successful fence')
        self.check_committed()
        self.ghost_snapshots[self.current] = self.ghost_map
        return 'stored'
    def publish(self, attempt, lost=False):
        if attempt.attempted and self.mutant != 'resubmit': raise Stale('submitted once')
        attempt.attempted = True
        if self.mutant != 'publish_floor_skip' and (
            attempt.knowledge.highest_epoch > attempt.original.epoch
            or attempt.knowledge.floor != attempt.original
        ): raise Stale('publication below shared authority')
        attempt.calls += 1
        if self.mutant!='cas_token_skip' and (self.current.token != attempt.original.token or self.current.binding != attempt.original.binding): return 'conflict'
        # Independent ghosts consume the submitted inputs, never candidate trees.
        before = self.ghost_map
        start = sum(count for _,_,count in self.ghost_log)
        self.ghost_log += attempt.inputs
        self.ghost_map = ghost_apply(before, attempt.edits)
        self.ghost_groups += (GhostGroup(start, start+sum(count for _,_,count in attempt.inputs), attempt.inputs, before, self.ghost_map),)
        prior_epoch=self.current.epoch
        self.current=replace(attempt.candidate,token=self.current.token+1)
        require(self.current.epoch>=prior_epoch,'stored epoch monotonicity','successful storage write','fence cannot be undone')
        if self.mutant=='split_publish' and len(attempt.inputs)==2:
            self.current=replace(self.current,index=Index(self.current.index.frames[:-1]))
        self.ghost_manifests[len(self.ghost_groups)] = self.current.seal.manifest
        self.check_committed()
        self.ghost_snapshots[self.current] = self.ghost_map
        if not lost: attempt.knowledge.floor = self.current
        return 'lost' if lost else 'stored'
    def resolve(self, attempt, knowledge, head, entries=2, pages=100):
        if not attempt.attempted and self.mutant != 'resolve_unsubmitted': raise Stale('resolve requires original submission')
        budget = Budget(100000 if self.mutant == 'resolve_budget_skip' else pages, self.mutant)
        try:
            if not self.observe(knowledge, head, budget): return 'unknown'
            if self.mutant == 'forget_floor': knowledge.floor = attempt.original
            if not self.successor(attempt.original, head, budget): return 'unknown'
            if head.index.end < attempt.candidate.index.end: return 'unknown'
            if self.mutant == 'numeric_durable': return 'durable'
            frames = tuple(f for f in head.index.frames if f.base >= attempt.original.index.end)
            found, cursor, mismatch = [], attempt.original.index.end, False
            for actual in frames[:entries]:
                if actual.base != cursor: return 'unknown'
                mismatch |= len(found) >= len(attempt.frames) or actual != attempt.frames[len(found)]
                found.append(actual)
                cursor += actual.count
                if self.mutant == 'first_only' and not mismatch: return 'durable'
                if self.mutant == 'early_reject' and mismatch: return 'rejected'
                if cursor >= attempt.candidate.index.end:
                    if mismatch or tuple(found) != attempt.frames: return 'rejected'
                    valid_candidate = (self.ancestry(attempt.candidate.seal.manifest, head.seal.manifest, budget)
                                       if self.mutant == 'resolve_candidate_skip' else self.successor(attempt.candidate,head,budget))
                    if not valid_candidate: return 'unknown'
                    if self.mutant == 'durable_floor_reset': knowledge.floor = attempt.original
                    return 'durable'
            return 'unknown'
        except Unavailable: return 'unknown'
    def scan(self, token, rows, work=100, output_bytes=100):
        if rows <= 0: raise ValueError('positive rows required')
        head = self.current if self.mutant == 'scan_latest' else token.cut
        try:
            if head.seal is None or not head.readable: raise Unavailable('scan source or uninitialized coverage')
            source = self.state(head)
            remaining = tuple((k,v) for k,v in source if token.lower <= k <= token.upper and k > token.after)
            returned = []
            used = 0
            complete = False
            for key,value in remaining:
                if work <= 0 or (head.seal.manifest,key) in self.missing_scan:
                    raise Unavailable('scan successor/work')
                work -= 1
                # Abstract key width=1; values0/1 have width1/2, value2 is empty.
                size = 1 + (0 if value == 2 else value+1)
                if used+size > output_bytes:
                    if not returned: raise OutputLimit('first row cannot fit')
                    break
                returned.append((key,value)); used += size
                if len(returned) == rows: break
            else:
                if work <= 0: raise Unavailable('scan EOF work')
                complete = True
            continuation = None if complete else replace(token, after=returned[-1][0])
            if continuation is not None and self.mutant == 'scan_skip_token':
                continuation = replace(continuation, after=continuation.after+1)
            return Page(tuple(returned), complete, continuation)
        except Unavailable:
            if self.mutant == 'scan_eof': return Page((), True, None)
            raise

def writer(machine, head=None):
    head = machine.current if head is None else head
    return Writer(head, Knowledge(head, head.epoch))
def observed_proof_facts(machine, original, floor, observed, entries, pages, durable_end=None, observation_only=False):
    # Independent oracle over successful ghost cuts. No verifier calls, source
    # frame comparisons, or numeric current-head shortcut establishes coverage.
    if observed not in machine.ghost_snapshots or not observed.readable:
        return False
    if observed.binding != original.binding or observed.binding != floor.binding:
        return False
    if observed.epoch < max(original.epoch, floor.epoch):
        return False
    committed = [group for group in machine.ghost_groups if group.end <= observed.index.end]
    required_refs = []
    cost = 0
    baselines = [(floor, True)] if observation_only else [(floor, True), (original, True)]
    if durable_end is not None:
        candidate = next((cut for cut in machine.ghost_snapshots if cut.index.end == durable_end), None)
        if candidate is None: return False
        baselines.append((candidate, True))
    for baseline, seal_read in baselines:
        if baseline not in machine.ghost_snapshots or baseline.index.end > observed.index.end:
            return False
        if baseline.index.end == observed.index.end and (baseline.index != observed.index or baseline.seal != observed.seal):
            return False
        if observed.seal is None:
            continue
        # A valid observed seal reads its manifest, then lineage reads the
        # starting manifest again and the independently selected skip stops.
        generation = len(committed)
        target = sum(group.end <= baseline.index.end for group in committed)
        target = max(target, 1)
        if seal_read:
            cost += 1
            required_refs.append(machine.ghost_manifests[generation])
        while True:
            cost += 1
            required_refs.append(machine.ghost_manifests[generation])
            if generation == target: break
            distance = generation - target
            jump = 1
            while jump * 2 <= distance: jump *= 2
            generation -= jump
    if pages < cost or any(reference in machine.missing for reference in required_refs):
        return False
    # Authenticate whole physical boundaries through the assigned end using
    # successful submitted ghost groups, independent of actual framed entries.
    available = []
    for group in committed:
        offset = group.start
        for _, _, count in group.inputs:
            if offset >= original.index.end:
                available.append((offset, offset + count))
            offset += count
    return available[:entries]

def emitted_resolution(machine, attempt, outcome, trace, observed,
                       entries, pages, floor, highest_epoch):
    group = next((group for group in machine.ghost_groups if group.start == attempt.original.index.end), None)
    matches = group is not None and group.end == attempt.candidate.index.end and group.inputs == attempt.inputs and group.after == attempt.expected_map
    facts = observed_proof_facts(machine, attempt.original, floor, observed, entries, pages,
                                 attempt.candidate.index.end if outcome == 'durable' else None)
    cursor = attempt.original.index.end
    if facts is not False:
        for start, end in facts:
            if start != cursor: break
            cursor = end
            if cursor >= attempt.candidate.index.end: break
    authenticated = facts is not False and observed.epoch >= highest_epoch and cursor >= attempt.candidate.index.end
    if outcome == 'durable' and observed.index.end == attempt.candidate.index.end:
        authenticated = authenticated and observed.index == attempt.candidate.index and observed.seal == attempt.candidate.seal
    if outcome == 'durable': require(authenticated and matches, 'durable original coupled content', *trace)
    if outcome == 'rejected': require(authenticated and not matches, 'complete incompatible rejection', *trace)
def resolve_checked(machine, attempt, knowledge, observed, trace, entries=2, pages=100):
    floor, highest = knowledge.floor, knowledge.highest_epoch
    outcome = machine.resolve(attempt, knowledge, observed, entries=entries, pages=pages)
    emitted_resolution(machine, attempt, outcome, trace, observed, entries, pages, floor, highest)
    expected_highest = max(highest,observed.epoch) if observed.binding == floor.binding else highest
    require(knowledge.highest_epoch == expected_highest,'resolution retains authority','sticky highest epoch',*trace)
    accepted = (observed.epoch >= expected_highest and
                observed_proof_facts(machine,attempt.original,floor,observed,entries,pages,observation_only=True) is not False)
    require(knowledge.floor == (observed if accepted else floor),
            'resolution retains authority','successful observation survives every later result',*trace)
    return outcome

def emitted_scan(machine, token, page, trace, rows, work, output_bytes):
    expected = tuple((k,v) for k,v in token.ghost if token.lower <= k <= token.upper and k > token.after)
    require(page.rows == expected[:len(page.rows)], 'scan original captured contents', *trace)
    require(rows > 0 and len(page.rows) <= rows, 'scan output constraints', *trace)
    widths = {0:1, 1:2, 2:0}
    require(sum(1+widths[value] for _,value in page.rows) <= output_bytes,
            'scan output constraints',*trace)
    if page.complete: require(page.rows == expected, 'scan verified completion', *trace)
    require(token.cut.seal is not None and token.cut.readable,'scan verified completion',*trace)
    touched = list(page.rows)
    if not page.complete and len(page.rows) < rows:
        require(len(expected) > len(page.rows),'scan verified completion',*trace)
        touched.append(expected[len(page.rows)])
    require(work >= len(touched) + int(page.complete)
            and not any((token.cut.seal.manifest,key) in machine.missing_scan for key,_ in touched),
            'scan verified completion',*trace)
    if page.complete:
        require(page.token is None,'scan continuation integrity',*trace)
    else:
        require(bool(page.rows) and page.token == replace(token, after=page.rows[-1][0]),
                'scan continuation integrity',*trace)

def scan_checked(machine, token, rows, work=100, output_bytes=100):
    page=machine.scan(token,rows,work=work,output_bytes=output_bytes)
    emitted_scan(machine,token,page,('captured scan',f'rows={rows}',f'work={work}',f'bytes={output_bytes}'),
                 rows,work,output_bytes)
    return page

EXPECTED = {
    'seal_pair_skip':'seal/index binding', 'zero_append_rewrite':'same-offset exact roots',
    'ancestry_skip':'exact manifest ancestry', 'ignore_budget':'proof exact budget',
    'wrong_transition':'assigned transition fields', 'wrong_map':'atomic ghost map', 'split_publish':'atomic ghost log', 'cas_token_skip':'stored epoch monotonicity', 'stale_cut_skip':'stale cut before writes',
    'bootstrap_nonempty':'nonempty bootstrap refused', 'resubmit':'single original submission',
    'reset_fence':'sticky unreadable fence', 'publish_floor_skip':'known floor forbids CAS', 'forget_floor':'resolution retains authority',
    'numeric_durable':'durable original coupled content', 'first_only':'durable original coupled content',
    'early_reject':'complete incompatible rejection', 'scan_latest':'scan original captured contents',
    'scan_eof':'scan verified completion', 'scan_skip_token':'scan continuation integrity',
    'resolve_budget_skip':'durable original coupled content', 'resolve_missing_skip':'durable original coupled content',
    'resolve_unsubmitted':'unsubmitted resolve refused',
    'resolve_candidate_skip':'durable original coupled content', 'durable_floor_reset':'resolution retains authority',
}

def run(mutant=None, max_pairs=8000):
    assert mutant is None or mutant in EXPECTED
    witnesses = {}
    def done(name, *steps): witnesses[name] = {'steps':list(steps), 'completed':True}
    unsubmitted=Machine(mutant); owner=writer(unsubmitted)
    pending=unsubmitted.prepare(owner,owner.original,((0,0,1),),())
    reads=unsubmitted.read_calls
    try: unsubmitted.resolve(pending,owner.knowledge,unsubmitted.current)
    except Stale: pass
    else: require(False,'unsubmitted resolve refused','no original submission')
    require(unsubmitted.read_calls==reads,'unsubmitted resolve refused','zero source reads')
    try: scan_checked(unsubmitted,Token(unsubmitted.current,()),1)
    except Unavailable: pass
    else: require(False,'scan verified completion','Uninitialized cannot prove empty')
    fenced=Machine(mutant); original=writer(fenced)
    stale=fenced.prepare(original,original.original,((0,0,1),),((0,1),))
    require(fenced.fence(original.original)=='stored','fence stored','original token','epoch advances')
    ghost=(fenced.ghost_log,fenced.ghost_map,fenced.ghost_groups)
    require(fenced.publish(stale)=='conflict','fenced original conflict','old prepared token','fence committed first')
    require((fenced.ghost_log,fenced.ghost_map,fenced.ghost_groups)==ghost,'conflict preserves ghosts','failed original CAS')
    done('fence-before-publication','prepare original','fence exact token','stale CAS conflicts','ghosts unchanged')
    inputs = ((0,0,1),(1,0,2))
    edits = ((0,1),(1,1),(2,1))
    m=Machine(mutant); w=writer(m)
    a=m.prepare(w,w.original,inputs,edits)
    b=m.prepare(w,w.original,inputs,((0,0),))
    require(not m.successor(w.original,replace(a.candidate,seal=b.candidate.seal),Budget(100,mutant)), 'seal/index binding','prepare same log different maps','pair index A with seal B')
    done('seal-pairing','same payload and range','different semantic edits','cross pair rejected')
    wrapped=Index(a.candidate.index.frames,1)
    rewrite=replace(a.candidate,index=wrapped,seal=Seal(a.candidate.seal.manifest,wrapped,wrapped.end),epoch=1)
    require(not m.successor(a.candidate,rewrite,Budget(100,mutant)), 'same-offset exact roots','valid wrapped index','matching new seal','zero append rejected')
    require(m.successor(a.candidate,replace(a.candidate,epoch=1),Budget(100,mutant)), 'fence preserves roots','epoch-only advance')
    require(not m.ancestry(a.candidate.seal.manifest,b.candidate.seal.manifest,Budget(100,mutant)), 'exact manifest ancestry','same generation different map')
    done('same-offset-and-ancestry','wrapped roots rejected','unchanged roots fenced','sibling ancestry rejected')
    # Nine committed generations exercise the level-three boundary.
    seeded=Machine(mutant); history=[]
    for generation in range(1,10):
        w=writer(seeded); p=seeded.prepare(w,w.original,((generation,0,1),),((generation%3,generation%2),))
        seeded.publish(p); history.append(p.candidate.seal.manifest)
    for generation, before in enumerate(history,1):
        needed=(9-generation).bit_count()+1
        exact=Budget(needed,mutant)
        require(seeded.ancestry(before,history[-1],exact) and exact.used==needed,'proof exact budget','nine generations',f'floor={generation}',f'N={needed}')
        try: seeded.ancestry(before,history[-1],Budget(needed-1,mutant))
        except Unavailable: pass
        else: require(False,'proof exact budget','nine generations',f'floor={generation}','N-1 must fail')
    done('logarithmic-proof','generation1..9','all floors','independent popcount+1','N/N-1')
    m=Machine(mutant); old=writer(m); p=m.prepare(old,old.original,inputs,edits); m.publish(p,lost=True)
    done('durable-before-reply-loss','original token CAS','independent ghosts','lost reply')
    try: m.publish(p)
    except Stale: pass
    require(p.calls==1,'single original submission','lost reply','second submit refused')
    later=m.prepare(writer(m),m.current,((2,1,1),),((2,None),)); m.publish(later)
    outcome=resolve_checked(m,p,old.knowledge,m.current,('stored lost reply','later coupled append','resolve original'))
    require(outcome=='durable','lost reply later resolution','later publication','original coupled content')
    done('lost-reply-later-resolution','durable lost reply','later map/log publication','original certificates and ancestry')
    # End-to-end resolution needs both observed-prefix proofs AND the final
    # prepared-candidate ancestry. Three generations cost 3+3+3 = 9 reads.
    budgeted=Machine(mutant); original_writer=writer(budgeted)
    original_attempt=budgeted.prepare(original_writer,original_writer.original,((0,0,1),),((0,1),))
    budgeted.publish(original_attempt,lost=True)
    for identity in (1,2):
        owner=writer(budgeted)
        appended=budgeted.prepare(owner,owner.original,((identity,0,1),),((identity,1),))
        budgeted.publish(appended)
    for pages, expected in ((9,'durable'),(8,'unknown')):
        outcome=resolve_checked(budgeted,original_attempt,Knowledge(original_attempt.original),budgeted.current,
                                ('resolution exact N/N-1','includes final candidate ancestry',str(pages)),pages=pages)
        require(outcome==expected,'resolution exact budget','3+3+3 reads',str(pages))
    required=budgeted.ghost_manifests[1]
    budgeted.missing.add(required)
    outcome=resolve_checked(budgeted,original_attempt,Knowledge(original_attempt.original),budgeted.current,
                            ('required manifest missing',),pages=9)
    require(outcome=='unknown','resolution missing source','required manifest missing')
    require(observed_proof_facts(budgeted,original_attempt.original,original_attempt.original,budgeted.current,2,9) is False,
            'independent resolution oracle','missing required proof facts refused')
    for forged in ('durable','rejected'):
        try:
            emitted_resolution(budgeted,original_attempt,forged,('forged classification with missing source',),
                               budgeted.current,2,9,original_attempt.original,0)
        except OracleFailure: pass
        else: require(False,'independent resolution oracle','missing source accepted forged '+forged)
    budgeted.missing.remove(required)
    restored=resolve_checked(budgeted,original_attempt,Knowledge(original_attempt.original),budgeted.current,
                             ('required manifest restored',),pages=9)
    require(restored=='durable','resolution source restoration','same original attempt')
    for forged in ('durable','rejected'):
        try:
            emitted_resolution(budgeted,original_attempt,forged,('forged classification with insufficient proof work',),
                               budgeted.current,2,5,original_attempt.original,0)
        except OracleFailure: pass
        else: require(False,'independent resolution oracle','insufficient work accepted forged '+forged)
    incompatible=budgeted.prepare(original_writer,original_writer.original,((0,1,1),),((0,1),))
    require(budgeted.publish(incompatible)=='conflict','original conflict','competing original submitted')
    for pages,expected in ((6,'rejected'),(5,'unknown')):
        outcome=resolve_checked(budgeted,incompatible,Knowledge(incompatible.original),budgeted.current,
                                ('complete competing resolution N/N-1',str(pages)),pages=pages)
        require(outcome==expected,'resolution exact budget','3+3 observed proofs, no final durable ancestry',str(pages))
    done('resolution-source-and-budget','N=9 Durable','N-1=8 Unknown includes final successor',
         'missing required manifest Unknown','restoration Durable','forged outcomes independently refused')

    wrapping=Machine(mutant); wrapping_owner=writer(wrapping)
    wrapping_attempt=wrapping.prepare(wrapping_owner,wrapping_owner.original,((0,0,1),),((0,1),))
    wrapping.publish(wrapping_attempt,lost=True)
    wrapped_index=replace(wrapping.current.index,layout=1)
    wrapped_observation=replace(wrapping.current,index=wrapped_index,
                                seal=replace(wrapping.current.seal,index=wrapped_index))
    # Explicit adverse observation: independently valid roots/content, same end.
    wrapping.ghost_snapshots[wrapped_observation]=wrapping.ghost_map
    result=resolve_checked(wrapping,wrapping_attempt,Knowledge(wrapping_attempt.original),wrapped_observation,
                           ('same-end valid wrapped roots','candidate successor must reject'))
    require(result=='unknown','resolution candidate roots','same end requires exact roots')
    retained=Knowledge(wrapping.current,wrapping.current.epoch)
    result=resolve_checked(wrapping,wrapping_attempt,retained,wrapped_observation,
                           ('retained genuine candidate','wrapped same-end observation'))
    require(result=='unknown' and retained.floor==wrapping.current,
            'resolution candidate roots','same-end rewrite cannot replace retained floor')

    fresh=writer(m); before_objects=len(m.objects)
    try: m.prepare(fresh,old.original,((2,1,1),),((0,0),))
    except Stale: pass
    else: require(False,'stale cut before writes','alias refreshed','old cut rejected')
    require(len(m.objects)==before_objects,'stale cut before writes','no immutable writes')
    for cut in (replace(fresh.original,token=fresh.original.token+1),
                replace(fresh.original,binding=(2,1,1)),replace(fresh.original,binding=(1,2,1)),replace(fresh.original,binding=(1,1,2))):
        try:m.prepare(fresh,cut,((2,1,1),),((0,0),))
        except Stale:pass
        else:require(False,'stale cut before writes','source/binding/literal/token mismatch')
    require(len(m.objects)==before_objects,'stale cut before writes','all cut mismatches write nothing')

    done('stale-cut-and-one-shot','storage version changed','alias refresh','old binding rejected','no writes')
    legacy=Machine(mutant); legacy.current=Head(index=Index((p.frames[0],))); legacy.ghost_snapshots[legacy.current]=()
    try: legacy.prepare(writer(legacy),legacy.current,((2,0,1),),((0,1),))
    except Stale: pass
    else: require(False,'nonempty bootstrap refused','legacy nonempty Uninitialized','no checkpoint promotion')
    done('bootstrap','nonempty Uninitialized refused')
    guarded = writer(m)
    guarded_attempt = m.prepare(guarded, guarded.original, ((3,0,1),), ((0,0),))
    knowledge=guarded.knowledge
    try: m.observe(knowledge,replace(m.current,epoch=7,readable=False),Budget(100,mutant))
    except Unavailable: pass
    require(knowledge.highest_epoch==7,'sticky unreadable fence','higher epoch','unreadable source','fence observed before proof')
    require(not m.observe(knowledge,m.current,Budget(100,mutant)),'sticky unreadable fence','old authority restored','no fresh family')
    before_cas = (m.current, m.ghost_log, m.ghost_map)
    try: m.publish(guarded_attempt)
    except Stale: pass
    require(guarded_attempt.calls == 0 and guarded_attempt.attempted
            and (m.current, m.ghost_log, m.ghost_map) == before_cas,
            'known floor forbids CAS','higher unreadable epoch','old authority restored','zero physical CAS')
    done('sticky-fence','unreadable higher observation','rollback refused','shared attempt knowledge','zero physical CAS')
    # An independently valid later head is retained even when the requested proof
    # is incomplete or complete incompatible coverage is established.
    for result_kind in ('unknown','rejected'):
        x=Machine(mutant); w=writer(x)
        target=x.prepare(w,w.original,inputs,edits)
        competing=x.prepare(writer(x),w.original,((0,0,1),(1,1,2)),edits)
        x.publish(competing)
        require(x.publish(target)=='conflict','original conflict','submit losing original before resolution')
        k=Knowledge(w.original,w.original.epoch)
        outcome=resolve_checked(x,target,k,x.current,('competing original','observe newer head',result_kind), entries=1 if result_kind=='unknown' else 2)
        require(outcome==result_kind,'partial proof remains unknown' if result_kind=='unknown' else 'complete competitor rejected','resolve complete boundaries')
        require(k.floor==x.current,'retain observed floor','observed valid newer head',result_kind,'floor must survive result')
        done('retained-floor-'+result_kind,'valid advanced floor',result_kind,'old original cannot relabel knowledge')
    # Mismatch before incomplete coverage must not become definite rejection.
    x=Machine(mutant); w=writer(x)
    target=x.prepare(w,w.original,inputs,edits)
    competing=x.prepare(writer(x),w.original,((0,1,1),(1,0,2)),edits)
    x.publish(competing)
    require(x.publish(target)=='conflict','original conflict','submit mismatching original before resolution')
    result=resolve_checked(x,target,Knowledge(w.original),x.current,('mismatching first entry','budget1 incomplete group'), entries=1)
    require(result=='unknown','complete incompatible rejection','mismatch before incomplete coverage')
    done('partial-mismatch','mismatch first','budget1','Unknown not Rejected')
    # Captured state and continuation survive deletion of an unread suffix key.
    x=Machine(mutant); w=writer(x); p=x.prepare(w,w.original,((0,0,1),),edits); x.publish(p)
    token=Token(x.current,x.ghost_map)
    first=scan_checked(x,token,1)
    q=x.prepare(writer(x),x.current,((1,1,1),),((1,0),(2,None))); x.publish(q)
    # Discard a completed response (caller cancellation); input token is immutable.
    discarded=scan_checked(x,first.token,10)
    replay=scan_checked(x,first.token,10)
    retried=scan_checked(x,first.token,10)
    require(replay==retried,'scan immutable replay','retry same input token')
    for broken in (replace(token,cut=replace(token.cut,readable=False)),token):
        try: page=scan_checked(x,broken,10,work=0 if broken==token else 100)
        except Unavailable: pass
        else: require(False,'scan verified completion','missing successor or exhausted work','must not Complete')
    done('captured-scan','3 ordered keys','unread key deleted later','cancel response','replay original suffix','missing/work errors')
    try: scan_checked(x,token,0)
    except ValueError: pass
    else: require(False,'scan positive rows','zero rows refused')
    exact=scan_checked(x,token,3)
    require(not exact.complete,'scan exact limit','exhaustion unproven')
    tail=scan_checked(x,exact.token,1)
    require(tail.complete and not tail.rows,'scan exact limit','empty Complete after final key')
    limited=scan_checked(x,token,10,output_bytes=3)
    require(limited.rows==((0,1),) and not limited.complete,'scan byte limit','prefix and original token')
    try: scan_checked(x,token,10,output_bytes=2)
    except OutputLimit: pass
    else: require(False,'scan byte limit','first row cannot fit')
    x.missing_scan.add((token.cut.seal.manifest,1))
    try:
        failed=scan_checked(x,token,10)
    except Unavailable: pass
    else: require(False,'scan verified completion','missing actual successor must fail whole page')
    x.missing_scan.clear()
    try:
        failed=scan_checked(x,token,10,work=1)
    except Unavailable: pass
    else: require(False,'scan verified completion','work after prefix must fail whole page')
    for forged, rows, work, byte_limit in (
        (Page(token.ghost,True,None),1,100,100),
        (Page(token.ghost,True,None),10,100,1),
        (Page(token.ghost,True,None),10,1,100),
    ):
        try: emitted_scan(x,token,forged,('forged scan limits',),rows,work,byte_limit)
        except OracleFailure: pass
        else: require(False,'independent scan oracle','forged output accepted')
    done('scan-byte-work-and-exact-limits','zero rows refused','first row too large','prefix byte stop',
         'exact rows then empty Complete','actual unread successor missing after prefix','work after prefix')

    # No-op and deletion-to-empty remain Covered, with immutable prior contents.
    empty_value=x.prepare(writer(x),x.current,((4,0,1),),((2,2),)); x.publish(empty_value)
    empty_token=Token(x.current,x.ghost_map,lower=2,upper=2)
    empty_page=scan_checked(x,empty_token,2,output_bytes=1)
    require(empty_page.rows==((2,2),),'empty value distinct from deletion','present zero-byte value')
    prior=x.current; prior_map=x.ghost_map
    noop=x.prepare(writer(x),x.current,((2,0,1),),()); x.publish(noop)
    require(x.current.seal is not None and x.ghost_map==prior_map,'noop Covered','nonempty log','no map edits')
    clear=x.prepare(writer(x),x.current,((3,0,1),),tuple((key,None) for key,_ in prior_map)); x.publish(clear)
    require(x.current.seal is not None and x.ghost_map==() and x.state(prior)==prior_map,'empty Covered','delete all state','old root retained')
    done('noop-and-empty','nonempty log noop','delete to empty Covered','old contents preserved')
    # A retained writer may prepare while storage has advanced elsewhere. Its
    # expected semantic result must still derive from its own original cut.
    stale_writer = writer(m)
    independent = writer(m)
    advance = m.prepare(independent, independent.original, ((4,0,1),), ((1,0),))
    m.publish(advance)
    stale_prepared = m.prepare(stale_writer, stale_writer.original, ((5,0,1),), ((0,0),))
    require(stale_prepared.expected_map == ghost_apply(m.ghost_snapshots[stale_writer.original], ((0,0),)),
            'original ghost snapshot','prepare after independent storage advance')
    require(m.publish(stale_prepared) == 'conflict','original conflict','old cut prepared after advance')
    # Exhaustive bounded pairs: every stored publication and classified result is
    # checked against ghosts independent of authored frames/manifests/seals.
    groups=[]
    for size in (1,2):
        for payloads in product((0,1),repeat=size):
            for counts in product((1,2),repeat=size):
                groups.append(tuple((i,p,c) for i,(p,c) in enumerate(zip(payloads,counts))))
    edit_sets=((),((0,0),),((0,1),),((1,None),),((0,1),(2,0)))
    checked=0
    for left,right,changes,left_wins,lost in product(groups,groups,edit_sets,(False,True),(False,True)):
        if checked>=max_pairs: raise ExplorationLimit(f'pair bound exhausted after {checked}')
        x=Machine(mutant); w=writer(x)
        target=x.prepare(w,w.original,left,changes)
        rival_writer=writer(x)
        rival=x.prepare(rival_writer,rival_writer.original,right,changes)
        winner,loser=(target,rival) if left_wins else (rival,target)
        result=x.publish(winner,lost=lost)
        require(result==('lost' if lost else 'stored'),'storage and reply separate','bounded pair',str(checked))
        saved=(x.current,x.ghost_log,x.ghost_map,x.ghost_groups)
        require(x.publish(loser)=='conflict','original conflict','two prepared original tokens')
        require((x.current,x.ghost_log,x.ghost_map,x.ghost_groups)==saved,'conflict preserves ghosts','losing CAS')
        for bound in (1,2):
            k=Knowledge(w.original)
            outcome=resolve_checked(x,target,k,x.current,('bounded competitor pair',str(checked),f'entries={bound}'), entries=bound)
        checked+=1
    done('exhaustive-competing-groups',f'{checked} input pairs','physical entries1/2','record counts1/2','independent Stored/Durable/Rejected oracles')
    return {'checked_pairs':checked,'seeded_generations':9,'witnesses':witnesses,
            'bounds':'3 ordered keys; values0/1/empty; payloads0/1; groups1/2; records1/2; 9-generation seed',
            'scope':'finite protocol abstraction; not Rust refinement, wire/provider/resource or release proof'}
