# Lucid Dream Baseball (LDB) — League Rules Summary

**Source:** https://ldbrules.lucidmetrics.com  
**Scoreboard/Stats:** https://lucidmetrics.com  
**Last updated:** March 2026

---

## League Structure
- **Type:** Dynasty, Head-to-Head
- **Teams:** 16 (4 divisions of 4)
  - Federal League: California Winter (NEO, CHOICE, WIND, ICHI), Iron & Oil (POLLOS, ROOF, FRY, DIPAS)
  - Union: All-American Girls (BALK, N8, PWRS, TONE), Honkbal Hoofdklasse (AIDS, WORK, CORN, IZZY)
- **Roster:** 28+ spots + Minor League / AA prospect roster
- **Auction budget:** $200m per team (annual)
- **Eligible players in annual auction:** Any unowned player
- **Salary cap:** $200m per team
- **Special features:** Salary caps, contract years, AA/minor league roster, taxi squad rules

---

## Scoring Categories — 6x6

### Hitting (6 categories)
| Category | Notes |
|---|---|
| HR | Home Runs |
| R | Runs |
| OBP | On-Base Percentage |
| OPS | On-Base + Slugging |
| aRBI | Adjusted RBI |
| aSB | Adjusted Stolen Bases (efficiency-weighted) |

**Key implications:**
- No AVG — pure contact hitters with low walk rates have diminished value
- OBP + OPS double-rewards plate discipline and power
- aSB penalizes inefficient base stealers; target high-volume + high-success guys

### Pitching (6 categories)
| Category | Notes |
|---|---|
| K | Strikeouts |
| HRA | Home Runs Allowed (lower is better) |
| aWHIP | Adjusted WHIP |
| VIJAY | Custom reliever metric (see formula below) |
| ERA | Earned Run Average |
| MGS | Modified Game Score — rewards deep, dominant starts |

**Key implications:**
- No Saves or Wins as standalone categories
- MGS heavily rewards aces who go deep; workhorses with 7+ inning outings are premium
- HRA makes fly-ball pitchers risky; ground ball / strikeout pitchers are gold
- VIJAY makes quality relievers (closers + setup men) genuinely valuable

---

## VIJAY Formula (Reliever Metric)
```
VIJAY = (((INN − (INNdGS × GS)) + (S × 3) + (HD × 3)) / 4) − ((BS + RL) × 2)
```

**Variables:**
- INN = Total innings pitched
- INNdGS = Innings per game start
- GS = Games started
- S = Saves (×3 weight)
- HD = Holds (×3 weight — equal value to saves)
- BS = Blown Saves (−2 penalty)
- RL = Reliever Losses (−2 penalty)

**Key implications:**
- Locked-in closers (high saves, low BS) are very valuable
- Elite setup men with lots of holds are equally valuable to closers
- Multi-inning relievers accumulate relief innings component
- Volatile closers with blown save risk are liabilities
- Budget ~$30–50 for 2–3 quality relievers (1 closer + 1–2 hold-heavy setup men)

---

## Draft Board Interpretation

**File:** `2026_LDB_Draft_Board_-_2026_Board.csv`

### Structure
The CSV is a wide matrix. Each team occupies **3 columns**: `Name | Salary ($M) | Contract Code`. Rows are organized by position group. The column order of teams (left to right) is:
CHOICE, ICHI, WIND, NEO, FRY, POLL, ROOF, IPA, IZZY, WORK, CORN, AIDS, TONES, NATE, PWRS, BALK

### Row Types
| Row Label | Meaning |
|---|---|
| `C, 1B, 2B, 3B, SS, OF, CF, RF` | Position players on active/major league roster |
| `UT` | Utility slots |
| `SP` | Starting pitchers |
| `RP` | Relief pitchers |
| `BN` | Bench slots |
| `SN1–SN6` | Minor league / Snake draft slots (promoted prospects) |
| `AA` | Farm system (no salary, $0.00) These players should be considered excluded from the draft pool for auction purposes |
| `MISC` | Trade adjustments, pick swaps, and side deals |
| `LUX` | Luxury tax assessed per team |
| `MCQ` | McQueeney performance penalties per team |
| `INN` | Innings-related penalties (if applicable) |
| `$$$` | Carry-over from prior year trades (negative = owed out) |
| `%` | Bottom rows tracking % of budget spent on Hit / Pit / Ben |

### Header Rows
- **Row 1:** Team abbreviations + remaining roster slots (number after team name)
- **Row 2:** GM names + remaining auction budget ($M)

### Contract Codes
| Code | Meaning |
|---|---|
| `K1, K2, K3` | Keeper contract — bought at auction, 3-year max. Number = years completed. K3 = final year → ROFR next auction |
| `S1, S2, S3, S4` | Promoted AA prospect (was Snake drafted) — 4-year contract. Number = years completed |
| `H1, H2, H3, H4` | Promoted AA prospect (was Hold drafted) — 4-year contract. Number = years completed |
| `HTH` | Home Town Hero — 1-year contract only, no renewal |

**RFA (Restricted Free Agent):** When a K3 contract expires, the owning team can match any auction bid to retain the player the following year.

### FRY Team Context (as of 2026 auction)
- **GM:** Jorve
- **Remaining budget:** $92.62M (effective $90.95M after -$1.67M carryover)
- **Remaining slots:** 23
- **Current payroll:** $109.05M → spending full budget hits exactly the $200M cap
- **Key keepers:** Acuña (K2), Devers (K2), Rooker (K2) — win-now window 2026–27
- **K3 expiries (ROFR):** Raleigh, Rasmussen
- **Critical needs:** SP (nearly empty), RP (zero VIJAY), SS, 2B

### League Budget Landscape (2026 auction)
| Team | GM | Rem. Budget | Payroll | Threat |
|---|---|---|---|---|
| ICHI | Jay/Paul | $213M | $7.5M | 🔴 Very high |
| POLL | Anton | $204M | $0M | 🔴 Very high |
| TONES | Michael | $197M | $0M | 🔴 Very high |
| WORK | Dubner | $170M | $50M | 🟡 High |
| WIND | Sean | $169M | $35M | 🟡 High |
| ROOF | Mark | $159M | $84M | 🟡 Moderate |
| AIDS | Josh | $160M | $53M | 🟡 Moderate |
| IPA | Chris/Vijay | $147M | $66M | 🟡 Moderate |
| PWRS | Starr | $149M | $26M | 🟡 Moderate |
| IZZY | Ray | $145M | $63M | 🟡 Moderate |
| NATE | Nathan | $146M | $49M | 🟡 Moderate |
| CHOICE | Brophy | $143M | $58M | 🟡 Moderate |
| BALK | Ian | $117M | $22M | 🟢 Lower |
| FRY | Jorve | $93M | $109M | 🟢 Constrained |
| NEO | Tim | $108M | $90M | 🟢 Constrained |
| CORN | Ryan | $131M | $114M | 🟢 Constrained |

Note: ICHI/POLL/TONES had not posted keepers yet as of this analysis — their true remaining budgets may be lower once keepers are declared.

---

## Auction Strategy Notes

### Budget Allocation ($300)
| Bucket | Spend | Target |
|---|---|---|
| Elite SP (2) | $80–100 | High K, low HR, deep outings (MGS) |
| Impact bats (3–4) | $80–90 | OBP+power combo, efficient SB |
| Mid-tier SP (2–3) | $40–50 | Solid ratios, ~180+ Ks, ground ball tendency |
| Quality RP (2–3) | $30–50 | Locked-in closer + elite setup man |
| Depth/prospects | $20–30 | AA-stashable upside |
| Reserve | $10 | Endgame $1 guys |

### Category Priority Rankings
**Hitting:** OPS > OBP > HR > R > aSB > aRBI  
**Pitching:** MGS > ERA > K > aWHIP > VIJAY > HRA

### Player Archetypes to Target
- SP aces with elite K rates, low HR/9, ability to go 7+ innings
- Hitters with OBP + power combo (double-scored via OBP and OPS)
- Fast players with high SB success rates
- Locked-in closers on contending teams
- High-leverage setup men who accumulate holds

### Player Archetypes to Avoid
- Pure contact/AVG hitters with no walks or power
- Fly-ball pitchers in homer-friendly parks
- Volatile closers with blown save risk
- Aging veterans on long contracts (dynasty flexibility matters)

### Dynasty-Specific Strategy
- Salary cap awareness is #1 edge — know who is cap-strapped and will be passive
- Nominate expensive players early that you don't want (drain competitors)
- Target ascending arc players aged 24–27 on controllable deals
- Prospects with minor league eligibility often go cheap — exploit this
- Middle market ($15–40) is where dynasty value lives; stars get overbid

