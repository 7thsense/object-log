---
title: Concepts
weight: 3
---

**Question this section answers:** Which piece do I touch for storage,
buffering, or offsets?

object-log is three cooperating surfaces:

```text
┌─────────────┐     ┌────────────┐     ┌─────────────┐
│  BlobStore  │◄────│ LogEngine  │────►│  Sequencer  │
│  (bytes)    │     │ (buffer)   │     │  (offsets)  │
└─────────────┘     └────────────┘     └─────────────┘
```

{{< cards >}}
  {{< card title="BlobStore" link="blob-store/" subtitle="Durable object port" >}}
  {{< card title="LogEngine" link="log-engine/" subtitle="Group-commit produce/fetch" >}}
  {{< card title="Sequencer" link="sequencer/" subtitle="Offset authority seam" >}}
  {{< card title="Durability" link="durability/" subtitle="Buffered / Durable / Sequenced" >}}
{{< /cards >}}

## What stays outside

| Concern | Owner |
|---------|--------|
| Kafka wire / record batch codec | Consumer protocol crate (e.g. heimq) |
| Producer id / epoch / sequence | `Sequencer::Meta` in the consumer |
| Epoch fencing / EOS | Sequencer implementor |
| Hot-tier local fsync | Consumer buffer in front of object-log |
| Retention *policy* | Consumer; object-log supplies `truncate_before` mechanism |
