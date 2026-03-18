# BRAND REFERENCE — Visual & Messaging Identity
# Claude: Read this file at the start of any task involving this person's brand.
# Apply all tokens, rules, and voice guidelines to ensure cohesive output.

---

## IDENTITY SNAPSHOT

- **Name/Initials:** JJ
- **Positioning:** Warm, bold, approachable thought leader / writer
- **Brand Vibe:** Institutional authority with human warmth. Technical credibility without coldness.

---

## COLOR PALETTE — "Ember & Gold"

Use on dark backgrounds by default.

| Token      | Hex       | Role                        | Usage                              |
|------------|-----------|-----------------------------|------------------------------------|
| Ember      | `#FF6B2B` | Primary brand color         | 25% — headers, accents, CTAs       |
| Saffron    | `#F5A623` | Supporting accent           | 5% — highlights, secondary details |
| Cream      | `#FAF0DC` | Text / light elements       | 10% — body text, light elements    |
| Charcoal   | `#1A1A1A` | Base background             | 60% — dominant background          |
| Dark       | `#0F0F0F` | Deep background (cover/hero)| Hero slides, cover pages           |

### Approved color combinations
- **Primary (dark):** Charcoal bg → Ember headings → Cream body → Saffron accents
- **Light:** Cream bg → Charcoal headings → Ember accents
- **Knockout:** Ember bg → White text → Saffron accents

### Never
- Pure black (`#000000`) or pure white (`#FFFFFF`) backgrounds
- Ember and Saffron at equal visual weight — Ember always dominates
- Low-contrast text (e.g. Muted on Dark for important content)

---

## TYPOGRAPHY — "Institutional Authority"

| Role              | Font            | Weight      | Notes                              |
|-------------------|-----------------|-------------|------------------------------------|
| Headings / Labels | IBM Plex Mono   | 700 Bold    | All caps + letter-spacing for labels |
| Accents / Code    | IBM Plex Mono   | 400 Regular | Monospace body, code blocks        |
| Body / Long-form  | EB Garamond     | 400 Regular | Primary reading font               |
| Quotes / Callouts | EB Garamond     | 400 Italic  | Pull quotes, featured statements   |
| Emphasis in body  | EB Garamond     | 600 Semi-bold | Inline emphasis                   |

Both fonts available free on Google Fonts.

### Type scale (slides/web)
- Display title: IBM Plex Mono 700, 36–44pt
- Section heading: IBM Plex Mono 700, 22–30pt
- Subheading / Label: IBM Plex Mono 400, 9–12pt, letter-spaced, ALL CAPS
- Body: EB Garamond 400, 13–16pt, leading 1.5–1.7×
- Caption / footer: Helvetica / system font, 7–9pt

### In code (CSS/JS)
```
--font-mono: 'IBM Plex Mono', 'Courier New', monospace;
--font-serif: 'EB Garamond', Georgia, serif;
```

---

## LOGO MARK — "The Teardrop"

Two closing quotation marks (" ") — JJ initials encoded inside typographic quote form.

### Construction
- Filled circle (outer) + hollow center ring + short curved tail curling down-left
- Left mark and right mark are identical, spaced ~60% of mark width apart
- Reads as closing quote marks at a distance, JJ initials up close

### Colors
- **Primary:** Ember `#FF6B2B` fill, Charcoal `#1A1A1A` inner hole
- **On light bg:** Charcoal fill, Cream inner hole
- **Knockout:** White fill, Ember bg
- **On Saffron:** Charcoal fill

### SVG path logic (for recreation)
```
Left mark:
  circle cx=28 cy=26 r=13 fill=Ember
  circle cx=28 cy=26 r=6  fill=bg (inner hole)
  path: M28,38 Q28,62 18,68  stroke=Ember stroke-width=9 linecap=round

Right mark (same, offset +34px on x):
  circle cx=62 cy=26 r=13 fill=Ember
  circle cx=62 cy=26 r=6  fill=bg
  path: M62,38 Q62,62 52,68  stroke=Ember stroke-width=9 linecap=round

ViewBox: 0 0 100 100
```

### Sizes
- Web / slides: 24px minimum recommended, 30–80px typical
- Footer mark: 18–24px
- Favicon: 16px minimum (simplify to solid marks at this size)
- Print: 12mm minimum

### Clear space
Minimum one dot-height (outer circle radius) on all sides.

### Never
- Recolor outside approved palette
- Stretch or distort proportions
- Add drop shadows or glows
- Place on low-contrast backgrounds
- Use below 16px

---

## DESIGN SYSTEM — Slide / UI Patterns

### Backgrounds
- Default: Charcoal `#1A1A1A`
- Hero / cover / closing: Dark `#0F0F0F`
- Card / elevated surface: `#221C16` (warm dark)
- Card border: `#2E2520`

### Accent bars & dividers
- Left edge accent bar: 4–8px wide, full height, Ember fill
- Top stripe accent: full width, 8–12px, Ember or Saffron
- Horizontal rule: 0.75pt, `#2E2520` (subtle) or Ember (emphasis)
- Short ember rule under attribution: 2–2.5" wide, 3–4pt

### Labels (eyebrow text above headings)
- Font: IBM Plex Mono, 8–11pt, ALL CAPS
- Color: Ember `#FF6B2B`
- Letter-spacing: wide (0.15–0.2em)
- No bold

### Cards
- Background: `#221C16`
- Border: 1pt solid `#2E2520`
- Top accent stripe: Ember (primary) or Saffron (secondary) or `#7A4A2E` (tertiary)
- Corner radius: 4–12px

### Footer (all content pages/slides)
- Background strip: `#111111`, height 22pt
- Left: 4pt Ember accent bar
- Center-left: "YOUR NAME" in IBM Plex Mono 7pt, Muted
- Right: Teardrop logo mark at 18–24px

### Decorative number (section dividers)
- IBM Plex Mono 700, 120–160pt
- Color: `#1A1410` (barely visible on dark bg)
- Positioned top-right, behind content

---

## VOICE & TONE

### Core voice statement
*"I'm someone who takes ideas seriously, admits what I don't know, and believes that honest questions are more valuable than confident answers."*

### Voice traits (always present)
| Trait             | What it means in practice                                                  |
|-------------------|----------------------------------------------------------------------------|
| Thoughtful        | Consider before speaking. Audience thinks alongside you, not below you.    |
| Honest            | Name uncertainty. Don't perform confidence you don't have.                 |
| Evidence-grounded | Bring data and sources. Flag when evidence is limited or mixed.            |
| Curious           | Ask better questions than most give answers. Share unresolved tensions.    |

### Tone by context
| Context           | Tone                                                         |
|-------------------|--------------------------------------------------------------|
| Keynote / Stage   | Warm, measured, purposeful — thinking out loud together      |
| Written articles  | Precise, reflective, evidence-led — smart friend over coffee |
| LinkedIn / Social | Direct, a little vulnerable, concise — starting a dialogue   |
| Podcast/interview | Conversational, generous, questioning                        |
| Email             | Clear, human, no performance — just be a person              |

### Six messaging principles
1. Say what you actually think — not the safe version
2. Name the uncertainty — "suggests" not "proves"
3. The question is the point — don't force tidy conclusions
4. Evidence is credibility — cite sources, show reasoning
5. Admit the mistakes — it signals genuine thinking
6. Write for one person — specificity creates resonance

### Words & phrases to avoid
Game-changer, leverage, synergy, disruptive, unlock potential, now more than ever,
best practices, world-class, move the needle, at the end of the day, deep dive,
thought leader, ecosystem, scalable, breakthrough results, rapidly evolving landscape

### Sentence-level patterns to avoid
- Overclaiming: "This will change everything" / "The definitive guide"
- Jargon wrapping a simple idea
- Performative humility ("I might be wrong but..." before confident assertion)
- Tidy 3-step frameworks for messy problems
- Audience flattery openers ("What an incredible crowd")
- False urgency ("Now more than ever...")

---

## QUICK TOKENS (copy-paste ready)

```
Ember:    #FF6B2B
Saffron:  #F5A623
Cream:    #FAF0DC
Charcoal: #1A1A1A
Dark:     #0F0F0F
Muted:    #6B5E52 (text at ~45% opacity on dark)
Card:     #221C16
Border:   #2E2520

Font mono: 'IBM Plex Mono', 'Courier New', monospace
Font serif: 'EB Garamond', Georgia, serif
```

---

## APPLYING THIS REFERENCE

When creating anything for this brand:

1. **Visual work** (slides, UI, graphics): Default to dark bg (`#1A1A1A`), Ember primary accent, IBM Plex Mono headings, EB Garamond body. Include Teardrop mark in footer.
2. **Written content** (posts, articles, emails): Thoughtful + honest voice. Use data, cite it, name limits. Ask questions. Avoid the word list above.
3. **Templates / documents**: Left ember accent bar, label → heading hierarchy, card-based content layout, footer with logo mark.
4. **Presentations**: Use brand-template.pptx as base. 8-slide layout. Dark bg. Teardrop in footer of every slide.
