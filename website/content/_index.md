---
title: object-log
layout: hextra-home
---

<div class="olog-home">
  <div class="olog-eyebrow">v0.3 · Rust · object storage</div>

  <h1 class="olog-display">Many writes.<br />Few objects.</h1>

  <p class="olog-lede">
    An embeddable log engine that group-commits opaque batches onto pluggable
    object storage. A sequencer you control assigns the offsets.
  </p>

  <div class="olog-actions">
    <a class="olog-btn olog-btn-primary" href="get-started/">Get started</a>
    <a class="olog-btn olog-btn-ghost" href="why/">Why this exists</a>
  </div>

  <div class="olog-seal" aria-hidden="false" role="img" aria-label="Diagram: many produce streams merge into one sealed object, then commit assigns offsets">
    <div class="olog-seal-label">Group-commit · seal · sequence</div>
    <svg viewBox="0 0 720 160" xmlns="http://www.w3.org/2000/svg" fill="none">
      <!-- produce streams -->
      <path class="stream stream-1" d="M20 28 C 160 28, 200 80, 320 80" stroke="#6BA3C7" stroke-width="1.5" opacity="0.85"/>
      <path class="stream stream-2" d="M20 56 C 150 56, 210 80, 320 80" stroke="#6BA3C7" stroke-width="1.5" opacity="0.7"/>
      <path class="stream stream-3" d="M20 84 C 140 84, 220 80, 320 80" stroke="#8B9BB4" stroke-width="1.5" opacity="0.65"/>
      <path class="stream stream-2" d="M20 112 C 150 112, 210 80, 320 80" stroke="#8B9BB4" stroke-width="1.25" opacity="0.55"/>
      <path class="stream stream-1" d="M20 132 C 170 132, 230 80, 320 80" stroke="#5a6b82" stroke-width="1.25" opacity="0.5"/>
      <!-- labels left -->
      <text x="20" y="22" fill="#8B9BB4" font-family="IBM Plex Mono, monospace" font-size="10">produce</text>
      <!-- sealed object -->
      <rect x="320" y="52" width="200" height="56" rx="4" fill="#1A2438" stroke="#C17A3A" stroke-width="2"/>
      <text x="420" y="78" text-anchor="middle" fill="#E0A05A" font-family="IBM Plex Mono, monospace" font-size="11">sealed object</text>
      <text x="420" y="94" text-anchor="middle" fill="#8B9BB4" font-family="IBM Plex Mono, monospace" font-size="10">BlobStore.put</text>
      <!-- commit -->
      <path d="M520 80 H 580" stroke="#3D9B8F" stroke-width="2"/>
      <circle cx="592" cy="80" r="10" fill="#0A0F1A" stroke="#3D9B8F" stroke-width="2"/>
      <text x="628" y="76" fill="#3D9B8F" font-family="IBM Plex Mono, monospace" font-size="11">commit</text>
      <text x="628" y="92" fill="#8B9BB4" font-family="IBM Plex Mono, monospace" font-size="10">offsets</text>
      <!-- tick marks on object = batches -->
      <line x1="350" y1="52" x2="350" y2="108" stroke="#C17A3A" stroke-width="1" opacity="0.35"/>
      <line x1="390" y1="52" x2="390" y2="108" stroke="#C17A3A" stroke-width="1" opacity="0.35"/>
      <line x1="430" y1="52" x2="430" y2="108" stroke="#C17A3A" stroke-width="1" opacity="0.35"/>
      <line x1="470" y1="52" x2="470" y2="108" stroke="#C17A3A" stroke-width="1" opacity="0.35"/>
    </svg>
    <p class="olog-seal-caption">
      Under load, <strong>PUT count ≈ flushes</strong> — not produces, not partitions.
    </p>
  </div>

  <div class="olog-pillars">
    <div class="olog-pillar">
      <div class="olog-pillar-kicker">Amortize</div>
      <h2>Cost is media ops</h2>
      <p>Linger packs many batches into one durable object. max_bytes is a safety ceiling, not the packing knob.</p>
    </div>
    <div class="olog-pillar">
      <div class="olog-pillar-kicker">Opaque</div>
      <h2>Bytes stay yours</h2>
      <p>No Kafka record model, no product schemas. Partition keys and payloads are uninterpreted.</p>
    </div>
    <div class="olog-pillar">
      <div class="olog-pillar-kicker">Seam</div>
      <h2>You own offsets</h2>
      <p>Sync Sequencer assigns ranges. Ship InMemory or Manifest—or plug a coordinator with typed Meta.</p>
    </div>
  </div>

  <section class="olog-section">
    <h2>How a produce resolves</h2>
    <div class="olog-flow"><span class="hi">produce</span>(partition, bytes, durability)
        │
        ▼
   buffer  ──linger / size / flush──►  <span class="hi">seal</span> multiplexed object
        │                                      │
        │                                      ▼
        │                               <span class="ok">BlobStore.put</span>   // durable barrier
        │                                      │
        │                                      ▼
        └──────────────────────►  <span class="ok">Sequencer.commit</span>  // offsets
                                           │
                                           ▼
                                fetch → get_range by BatchLocation</div>
  </section>

  <section class="olog-section">
    <h2>Trust, not marketing</h2>
    <dl class="olog-trust">
      <dt>Package</dt>
      <dd><a href="https://crates.io/crates/object-log">crates.io/object-log</a> · 0.3.x</dd>
      <dt>API</dt>
      <dd><a href="https://docs.rs/object-log">docs.rs/object-log</a></dd>
      <dt>S3 evidence</dt>
      <dd>MinIO (CI) + Garage (operator suite) — TD-002 in-repo</dd>
      <dt>Source</dt>
      <dd><a href="https://github.com/7thsense/object-log">github.com/7thsense/object-log</a></dd>
    </dl>
  </section>

  <section class="olog-section">
    <h2>Next</h2>
{{< cards >}}
  {{< card title="Get started" link="get-started/" subtitle="cargo add → produce and fetch" >}}
  {{< card title="Concepts" link="concepts/" subtitle="BlobStore · engine · sequencer" >}}
  {{< card title="Reference" link="reference/" subtitle="API surface and CLI" >}}
{{< /cards >}}
  </section>
</div>
