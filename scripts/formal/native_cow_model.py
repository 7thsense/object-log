"""Finite COW/prefix/content-resolution refinement, not Rust/provider proof.

Leaf/fanout=2, maximum height=2; ideal injective immutable page commitments.
The independent flat ghost history is updated only after successful exact-token
CAS. Storage publication and observed replies are separate. Immutable page
availability is an assumption; retained structural values prove value preservation,
not reclamation safety. Certificate identity is abstract (not separate wire logical/
physical identities or every byte-length field). No codec, semantic,
RSS or historical-writer-epoch claim is made here.
"""
from dataclasses import dataclass, replace
from itertools import product

@dataclass(frozen=True)
class Certificate:
    base: int
    count: int
    identity: int
    byte_start: int
    payload: int
    @property
    def end(self): return self.base + self.count

@dataclass(frozen=True)
class Page:
    height: int
    slots: tuple

# Structural values stand for ideal injective immutable descriptors.
def flatten(page):
    if page is None: return ()
    if page.height == 0: return page.slots
    return tuple(item for child in page.slots for item in flatten(child))

def valid_shape(page):
    if page is None: return True
    if not 0 <= page.height <= 2 or not 1 <= len(page.slots) <= 2: return False
    if page.height == 0:
        if not all(isinstance(e, Certificate) and e.base >= 0 and e.count > 0 for e in page.slots): return False
    else:
        if not all(isinstance(child, Page) and child.height + 1 == page.height and valid_shape(child) for child in page.slots): return False
    entries = flatten(page)
    return all(left.end == right.base for left, right in zip(entries, entries[1:]))

def cow(page, entry, mutant=None):
    if page is None: return Page(0, (entry,)), None
    if page.height == 0:
        if len(page.slots) < 2: return Page(0, page.slots + (entry,)), None
        if mutant == "drop_split": return page, None
        return page, Page(0, (entry,))
    left, right = cow(page.slots[-1], entry, mutant)
    children = page.slots[:-1] + (left,)
    if right is None: return Page(page.height, children), None
    if len(children) < 2: return Page(page.height, children + (right,)), None
    return Page(page.height, children), Page(page.height, (right,))

def prepare(page, entry, mutant=None):
    left, right = cow(page, entry, mutant)
    root = left if right is None else Page(left.height + 1, (left, right))
    if mutant == "reverse_growth" and right is not None: root = Page(left.height + 1, (right, left))
    return root if root.height <= 2 else None

def prefix(before, after, mutant=None):
    if mutant == 'prefix_skip': return True
    if before is None: return True
    if after is None: return False
    for _ in range(5):
        if before == after: return True
        old, new = flatten(before), flatten(after)
        if after.height < before.height or new[0].base != old[0].base or new[-1].end < old[-1].end: return False
        if after.height > before.height:
            if flatten(after.slots[0])[-1].end < old[-1].end: return False
            after = after.slots[0]
            continue
        if before.height == 0: return after.slots[:len(before.slots)] == before.slots
        if len(after.slots) < len(before.slots): return False
        if mutant != 'nonlast_skip' and before.slots[:-1] != after.slots[:len(before.slots)-1]: return False
        before, after = before.slots[-1], after.slots[len(before.slots)-1]
    return False

@dataclass(frozen=True)
class Head:
    token: int
    epoch: int
    root: Page | None
    ghost: tuple
    scope: int = 1
    readable: bool = True

def publish(current, original, candidate, certificate):
    if candidate is None or current.token != original.token: return current, False
    # The ghost oracle does not derive history from the new tree.
    return Head(current.token + 1, current.epoch, candidate, original.ghost + (certificate,)), True

def resolve(original, current, certificate, mutant=None):
    if current.scope != original.scope or current.epoch < original.epoch: return 'unknown'
    if not current.readable and mutant != 'absence_skip': return 'unknown'
    modeled = flatten(current.root)
    if mutant == 'numeric_head' and modeled and modeled[-1].end >= certificate.end: return 'durable'
    if not prefix(original.root, current.root, mutant): return 'unknown'
    entries = flatten(current.root)
    if not entries or entries[-1].end < certificate.end: return 'unknown'
    found = next((e for e in entries if e.base <= certificate.base < e.end), None)
    if found is None: return 'unknown'
    matches = found == certificate
    if mutant == 'certificate_base': matches = found.base == certificate.base
    if matches: return 'durable'
    if found.base == certificate.base and found.end >= certificate.end: return 'rejected'
    return 'unknown'

def prepare_group(original, certificates):
    root = original.root
    for certificate in certificates:
        root = prepare(root, certificate)
        if root is None: return None
    return root

def publish_group(current, original, candidate, certificates):
    if candidate is None or current.token != original.token: return current, False
    return replace(current, token=current.token + 1, root=candidate,
        ghost=original.ghost + certificates), True

def resolve_group(original, current, certificates, budget, missing=(), mutant=None):
    if not certificates or budget <= 0: return 'unknown'
    if current.scope != original.scope or current.epoch < original.epoch or not current.readable: return 'unknown'
    if mutant != "group_prefix_skip" and not prefix(original.root, current.root): return 'unknown'
    cursor, end = certificates[0].base, certificates[-1].end
    ordinal, mismatch = 0, False
    for _ in range(budget):
        if cursor in missing: return 'unknown'
        actual = next((e for e in flatten(current.root) if e.base <= cursor < e.end), None)
        if actual is None or actual.base != cursor: return 'unknown'
        mismatch |= ordinal >= len(certificates) or (actual != certificates[ordinal] and not (mutant == "group_first_only" and ordinal > 0))
        if mismatch and mutant == 'group_early_reject': return 'rejected'
        ordinal += 1
        cursor = actual.end
        if cursor >= end:
            return 'durable' if not mismatch and ordinal == len(certificates) and cursor == end else 'rejected'
    return 'unknown'

@dataclass(frozen=True)
class Attempt:
    original: Head
    certificate: Certificate
    candidate: Page | None
    submitted: bool = False
    reply: str = 'pending'
    submissions: int = 0

def submit(attempt, current, store=True, mutant=None):
    if attempt.submitted and mutant != 'resubmit': return current, attempt, False
    updated = replace(attempt, submitted=True, submissions=attempt.submissions+1)
    if not store: return current, updated, False
    durable, stored = publish(current, attempt.original, attempt.candidate, attempt.certificate)
    return durable, updated, stored

def reply(attempt, stored, lost):
    assert attempt.submitted
    return replace(attempt, reply='unknown' if lost else ('stored' if stored else 'conflict'))

def run(mutant=None):
    checked = 0
    witnesses = {}
    def record(name, *steps):
        witnesses.setdefault(name, {'steps': list(steps)})
    retained = set()
    for size in range(9):
        for symbols in product(range(2), repeat=size):
            head = Head(0, 3, None, ())
            snapshots = []
            for symbol in symbols:
                base = head.ghost[-1].end if head.ghost else 0
                entry = Certificate(base, symbol + 1, symbol, symbol, symbol)
                original = head
                candidate = prepare(original.root, entry, mutant)
                assert candidate is not None
                assert valid_shape(candidate), "COW-topology-must-be-valid"
                retained.add(candidate)
                head, stored = publish(head, original, candidate, entry)
                assert stored and flatten(head.root) == head.ghost, 'COW-publication-must-equal-independent-history'
                assert prefix(original.root, head.root, mutant)
                # Lost/not-stored observations do not alter durable ghost state.
                assert resolve(original, original, entry, mutant) == 'unknown'
                assert resolve(original, head, entry, mutant) == 'durable'
                if original.root is not None: record('nonempty-prefix', 'capture-nonempty', 'append', 'ghost-equality', 'prefix-preserved')
                record('height-' + str(head.root.height), 'append', 'ghost-equality', 'height='+str(head.root.height))
                snapshots.append((head.root, head.ghost))
                checked += 1
            for old_root, old_ghost in snapshots:
                assert old_root in retained and flatten(old_root) == old_ghost
                if old_root != head.root:
                    record("old-root-value-preserved", "captured-height="+str(old_root.height), "current-height="+str(head.root.height), "old-certificates="+repr(old_ghost), "read-old-value-equals-ghost")
                assert prefix(old_root, head.root, mutant)
            if size == 8:
                before = head
                ninth = Certificate(head.ghost[-1].end, 1, 0, 0, 0)
                candidate = prepare(head.root, ninth)
                assert candidate is None
                head, stored = publish(head, before, candidate, ninth)
                assert not stored and head == before
                record('ninth-explicit-rejection', 'eight-entries', 'prepare-ninth', 'reject', 'head-unchanged')
    initial = Head(0, 3, None, ())
    first = Certificate(0, 2, 0, 0, 0)
    head, _ = publish(initial, initial, prepare(None, first), first)
    second = Certificate(2, 1, 1, 1, 1)
    later, _ = publish(head, head, prepare(head.root, second), second)
    assert resolve(head, later, second, mutant) == 'durable'
    rewritten = Page(0, (replace(first, identity=1), second))
    changed = replace(later, root=rewritten)
    assert resolve(head, changed, second, mutant) == 'unknown', 'rewritten-prefix-must-remain-unknown'
    record('rewritten-prefix-unknown', "scenario-prepared", "independent-assertion-completed")
    for field in ['identity', 'byte_start', 'payload', 'count']:
        competitor = replace(first, **{field: getattr(first, field)+1})
        other = Head(1, 3, Page(0, (competitor,)), (competitor,))
        assert resolve(initial, other, first, mutant) == 'rejected', 'incompatible-complete-certificate-must-reject'
    record("complete-competitor-rejected", "same-assigned-base", "independent-identity-range-count-payload-mismatches", "authenticated-complete-occupancy-rejected")
    # Same epoch with a different version token still rejects stale CAS.
    current = head
    unchanged, stored = publish(current, initial, prepare(None, first), first)
    assert not stored and unchanged == current
    record('same-epoch-stale-token', "scenario-prepared", "independent-assertion-completed")
    assert resolve(head, replace(later, epoch=2), second, mutant) == 'unknown'
    # Seeded canonical subtree permits intermediate split into parent with room.
    leaves = tuple(Page(0, (Certificate(n,1,n,0,n), Certificate(n+1,1,n+1,0,n+1))) for n in [0,2])
    full = Page(1, leaves)
    seeded = Page(2, (full,))
    grown = prepare(seeded, Certificate(4,1,4,0,4))
    seeded_ghost = tuple(Certificate(n,1,n,0,n) for n in range(4))
    appended = Certificate(4,1,4,0,4)
    assert grown is not None
    assert valid_shape(grown), "seeded-COW-topology-must-be-valid"
    assert len(grown.slots)==2 and prefix(seeded, grown, mutant)
    assert flatten(grown) == seeded_ghost + (appended,), 'seeded split content differs from independent history'
    record('intermediate-split-parent-room', "scenario-prepared", "independent-assertion-completed")
    rewritten_branch = Page(1, (Page(0, (replace(leaves[0].slots[0],payload=9), leaves[0].slots[1])), leaves[1]))
    assert not prefix(full, rewritten_branch, mutant), 'non-last-subtree-rewrite-must-fail'
    attempt = Attempt(head, second, prepare(head.root, second))
    durable, submitted, stored = submit(attempt, head)
    assert stored and durable.ghost == head.ghost + (second,)
    assert submitted.reply == 'pending'
    observed = reply(submitted, stored, lost=True)
    assert observed.reply == 'unknown' and durable.ghost == head.ghost + (second,)
    record('storage-before-lost-reply', "scenario-prepared", "independent-assertion-completed")
    third = Certificate(second.end, 1, 2, 0, 2)
    advanced, stored_third = publish(durable, durable, prepare(durable.root, third), third)
    assert stored_third and advanced.ghost == head.ghost + (second, third)
    assert resolve(head, advanced, second, mutant) == 'durable', 'lost stored reply must resolve through a later append'
    record('lost-stored-later-append', "scenario-prepared", "independent-assertion-completed")
    unchanged, repeated, stored_again = submit(observed, durable, mutant=mutant)
    assert not stored_again and unchanged == durable and repeated == observed, 'publication-must-not-resubmit'
    record('submitted-once', "scenario-prepared", "independent-assertion-completed")
    not_stored, submitted, stored = submit(attempt, head, store=False)
    lost = reply(submitted, stored, lost=True)
    assert lost.reply == 'unknown' and not_stored == head
    assert resolve(head, not_stored, second, mutant) == 'unknown'
    record('lost-not-stored-unchanged', "scenario-prepared", "independent-assertion-completed")
    for inaccessible in [replace(later, readable=False), replace(later, root=None, readable=False)]:
        assert resolve(head, inaccessible, second, mutant) == 'unknown', 'inaccessible-page-must-remain-unknown'
        record('missing-or-corrupt-unknown', "scenario-prepared", "independent-assertion-completed")
    assert resolve(head, replace(later, scope=2), second, mutant) == 'unknown'
    record('scope-regression-unknown', "scenario-prepared", "independent-assertion-completed")
    assert resolve(head, initial, second, mutant) == 'unknown'
    partial = replace(first, count=1)
    occupied = Head(1, 3, Page(0, (partial,)), (partial,))
    assert resolve(initial, occupied, first, mutant) == 'unknown'
    record('partial-occupancy-unknown', "scenario-prepared", "independent-assertion-completed")
    group = (Certificate(0, 2, 10, 0, 10), Certificate(2, 3, 11, 0, 11))
    candidate = prepare_group(initial, group)
    grouped, stored = publish_group(initial, initial, candidate, group)
    assert stored and valid_shape(grouped.root) and flatten(grouped.root) == grouped.ghost == group
    stale, stored = publish_group(grouped, initial, candidate, group)
    assert not stored and stale == grouped
    record('group-all-or-none', 'unequal-counts', 'exact-token-CAS-and-stale-rejection')
    assert resolve_group(initial, grouped, group, 1) == 'unknown'
    assert resolve_group(initial, grouped, group, 2) == 'durable'
    record('group-proof-budget', 'N-minus-one-unknown', 'N-durable')
    third_group = Certificate(5, 1, 12, 0, 12)
    advanced, stored = publish(grouped, grouped, prepare(grouped.root, third_group), third_group)
    assert stored and valid_shape(advanced.root) and resolve_group(initial, advanced, group, 2) == 'durable'
    record('group-later-append-resolution', 'original-ordered-certificates-preserved')
    for competitors in [(Certificate(0, 5, 20, 0, 20),),
            tuple(Certificate(n, 1, 20+n, 0, 20+n) for n in range(5))]:
        competing, stored = publish_group(initial, initial, prepare_group(initial, competitors), competitors)
        assert stored and valid_shape(competing.root) and resolve_group(initial, competing, group, len(competitors)) == 'rejected'
    record('group-merged-and-split', 'complete-actual-boundaries-authenticated')
    split = tuple(Certificate(n, 1, 20+n, 0, 20+n) for n in range(5))
    competing, _ = publish_group(initial, initial, prepare_group(initial, split), split)
    assert resolve_group(initial, competing, group, 5, missing=(1,), mutant=mutant) == 'unknown', 'group-mismatch-before-missing-must-remain-unknown'
    partial, _ = publish_group(initial, initial, prepare_group(initial, split[:2]), split[:2])
    assert resolve_group(initial, partial, group, 5, mutant=mutant) == 'unknown'
    record('group-mismatch-before-incomplete', 'early-mismatch-missing-later-entry', 'partial-coverage')
    prefix_entry = Certificate(0, 1, 30, 0, 30)
    nonempty, _ = publish(initial, initial, prepare(None, prefix_entry), prefix_entry)
    after_prefix = (Certificate(1, 2, 10, 0, 10), Certificate(3, 3, 11, 0, 11))
    committed, _ = publish_group(nonempty, nonempty, prepare_group(nonempty, after_prefix), after_prefix)
    rewritten_entries = (replace(prefix_entry, identity=31),) + after_prefix
    rewritten = replace(committed, root=prepare_group(initial, rewritten_entries), ghost=rewritten_entries)
    assert valid_shape(rewritten.root)
    assert resolve_group(nonempty, rewritten, after_prefix, 2, mutant=mutant) == 'unknown', 'group-rewritten-original-prefix-must-remain-unknown'
    record('group-nonempty-prefix-rewrite', 'assigned-certificates-unchanged', 'original-prefix-rewritten')
    wrong_last = (group[0], replace(group[1], payload=99))
    incompatible, _ = publish_group(initial, initial, prepare_group(initial, wrong_last), wrong_last)
    assert valid_shape(incompatible.root)
    assert resolve_group(initial, incompatible, group, 2, mutant=mutant) == 'rejected', 'group-wrong-final-certificate-must-reject'
    record('group-matching-first-wrong-last', 'identical-ranges-counts', 'final-payload-changed')
    return {'checked_appends': checked, 'retained_roots': len(retained), 'witnesses': sorted(witnesses), 'completed_witnesses': witnesses}

EXPECTED = {
    'group_prefix_skip': 'group-rewritten-original-prefix-must-remain-unknown',
    'group_first_only': 'group-wrong-final-certificate-must-reject',
    'group_early_reject': 'group-mismatch-before-missing-must-remain-unknown',
    'prefix_skip': 'rewritten-prefix-must-remain-unknown',
    'nonlast_skip': 'non-last-subtree-rewrite-must-fail',
    'numeric_head': 'rewritten-prefix-must-remain-unknown',
    'certificate_base': 'incompatible-complete-certificate-must-reject',
    'absence_skip': 'inaccessible-page-must-remain-unknown',
    'resubmit': 'publication-must-not-resubmit',
    'drop_split': 'COW-publication-must-equal-independent-history',
    'reverse_growth': 'COW-topology-must-be-valid',
}

if __name__ == '__main__':
    import json
    positive = run()
    killed=[]
    for mutant in EXPECTED:
        try: run(mutant)
        except AssertionError as error:
            assert str(error) == EXPECTED[mutant], (mutant, str(error))
            killed.append(mutant)
        else: raise AssertionError('escaped model mutant: '+mutant)
    print(json.dumps({'passed':True, **positive, 'mutants_killed':killed}))
