# DESIGN.md — object-log microsite

Interface system for the public product site. Implements the HELIX
`product-microsite-ia` concern with a **custom skin** over Hugo/Hextra — not
default theme chrome.

**Subject:** embeddable Rust log engine for object storage.  
**Audience:** infrastructure engineers who already know offsets and S3 cost.  
**Page job (home):** make the group-commit thesis obvious in under five seconds.

---

## Aesthetic direction

**Name:** *Cold storage bay.*

Object storage is industrial media, not a pastel SaaS dashboard. The site should
feel like a clean ops console in a dim room: cool ink panels, precise mono
readouts, and one warm metal accent when something becomes **durable**
(the seal).

| Axis | Choice | Why |
|------|--------|-----|
| Mood | Cool industrial, restrained | Matches durability / cost seriousness |
| Light | **Dark-first** | Storage bay; Hextra light mode still works but secondary |
| Signature | **Seal rail** | Many thin produce lines → one thick sealed object (the product thesis) |
| Risk | Copper-on-ink + Syne display | Not Inter/blue SaaS; not acid-green cyber; not cream/serif lifestyle |

**Not this design:** generic Hextra blue hero, feature-grid-of-six with stock
icons, or “AI purple gradient.”

---

## Tokens

### Color

| Token | Hex | Role |
|-------|-----|------|
| `ink` | `#0A0F1A` | Page background |
| `panel` | `#141C2B` | Cards, sidebar, elevated surfaces |
| `panel-2` | `#1A2438` | Hover / inset |
| `line` | `#243049` | Borders, rules |
| `mute` | `#8B9BB4` | Secondary text |
| `chalk` | `#E8EDF5` | Primary text |
| `copper` | `#C17A3A` | Brand / durable seal / primary CTA |
| `copper-hi` | `#E0A05A` | Hover / focus accent |
| `ice` | `#6BA3C7` | Links secondary, fetch/read path |
| `good` | `#3D9B8F` | Success / sequenced |

Hextra primary scale is driven by:

```css
--primary-hue: 28deg;        /* copper */
--primary-saturation: 55%;
--primary-lightness: 48%;
```

### Type

| Role | Face | Use |
|------|------|-----|
| Display | **Syne** (600–800) | Hero, section titles — geometric, slightly odd |
| Body | **Source Sans 3** | Prose, UI |
| Data | **IBM Plex Mono** | Code, offsets, badges, tables |

Scale (approx):

| Step | Size | Weight | Face |
|------|------|--------|------|
| display | clamp(2.4rem, 5vw, 3.75rem) | 700 | Syne |
| h1 | 1.85rem | 700 | Syne |
| h2 | 1.35rem | 650 | Syne |
| body | 1.05rem / 1.65 | 400 | Source Sans 3 |
| small | 0.875rem | 400–500 | Source Sans 3 / Mono |
| micro | 0.72rem | 500 | Mono, uppercase tracking |

### Spacing

4 / 8 / 12 / 16 / 24 / 32 / 48 / 72 / 96  
Content measure: ~40rem prose; home hero wider.

### Radius & elevation

- Radius: **6px** cards, **999px** pills only for micro-badges  
- Shadows: soft ink-tinted (`0 12px 40px rgba(0,0,0,.35)`), not white glows  
- Borders: 1px `line`, copper 2px only for active nav rail

---

## Navigation and active state

| Surface | Component | Active cue (visible) | Semantic |
|---------|-----------|----------------------|----------|
| Top nav | Main menu links | Copper underline + chalk text | `aria-current="page"` when Hextra sets it |
| Sidebar | Section tree | **2px copper left rail** + panel-2 fill + chalk | active item class + focus ring |
| TOC | Right headings | Ice text, no heavy decoration | quiet |

Focus: `outline: 2px solid copper-hi; outline-offset: 2px` on interactive controls.

---

## Homepage structure

```
┌─────────────────────────────────────────────────────┐
│ nav (logo · sections · github)                      │
├─────────────────────────────────────────────────────┤
│ eyebrow mono · version                              │
│ DISPLAY headline                                    │
│ body thesis (2 lines max)                           │
│ [Get started]  [Why]                                │
│                                                     │
│ ══════ SEAL RAIL (signature SVG) ═════════════════  │
│   produce streams → sealed object + offset ticks    │
├─────────────────────────────────────────────────────┤
│ three principles (not six feature cards)            │
│ how it works (compact)                              │
│ trust strip                                         │
│ next cards                                          │
└─────────────────────────────────────────────────────┘
```

Hero is a **thesis**, not a marketing slogan. The seal rail is the one memorable
device; everything else stays quiet.

---

## Content pages

- Dark panel body; code blocks with deeper ink + mono  
- Tables: muted headers, copper hairline under thead  
- Cards: panel bg, line border, hover copper border  
- H1: Syne; avoid double H1 in markdown body  

---

## Motion

- Seal rail: optional subtle stroke draw on load (`prefers-reduced-motion: reduce` → static)  
- Link hover: color only, 120ms  
- No parallax, no continuous ambient particles  

---

## Voice (see also product-voice.md)

- Precise nouns: BlobStore, LogEngine, Sequencer, Durability  
- Honest layer boundaries (Kafka lives above)  
- No hype, no emoji, sentence case  

---

## Non-goals

- Runtime architecture, crate API design, or HELIX process docs beyond marketing  
- Light-mode-first branding (light mode is a fallback restyle, not the brand)  
- Pixel-perfect recreation of unrelated products  

## Implementation map

| File | Responsibility |
|------|----------------|
| `website/DESIGN.md` | This document |
| `website/assets/css/variables.css` | Primary hue + layout widths |
| `website/assets/css/custom.css` | Full skin + seal rail + type |
| `website/layouts/index.html` | Custom home (optional override) |
| `website/layouts/partials/custom/head-end.html` | Fonts (if partial exists) |
| `docs/helix/01-frame/product-voice.md` | Copy system |
