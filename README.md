# SING Pipeline Scheduler

**Kumar Lab — The Jackson Laboratory**
Scalable and Systematic Neurobiology of NPD Risk Genes (SING)
NIH-funded, 2024–2029

---

## Overview

The SING Pipeline Scheduler is a Python/tkinter GUI tool that automates harvest scheduling, Envision tagging prep, sample tracking, and output file generation for the SING project. It processes animal colony data exported from Climb/Envision and produces a complete set of Excel and CSV files ready for the harvest workflow.

Built and maintained by Tim Leach, Research Assistant, Kumar Lab.

---

## Files

| File | Version | Status | Notes |
|---|---|---|---|
| `sing_pipeline.py` | v1.7 | Stable | Previous production version — fallback if needed |
| `sing_pipeline_v2.py` | v2.2 | Current | Active production version |
| `sing_common.py` | v2.2 | Shared | Utility functions imported by all scripts — must be on the Python path |
| `Label_generator.py` | v2.2 | Standalone | Label reprints — run independently |
| `Deliverables_Sheet_Export.py` | v2.2 | Standalone | Lab data export — run independently |
| `Climb_to_Envision_Translation.py` | v2.2 | Standalone | Envision tag file — run independently |

---

## Requirements

```
Python 3.9+
pandas
openpyxl
tqdm (optional — gracefully falls back if not installed)
tkinter (included with standard Python)
```

Install dependencies:
```bash
pip install pandas openpyxl tqdm
```

---

## Shared Utilities (`sing_common.py`)

`sing_common.py` lives at:
```
Z:\kumarlab-new\Tim Leach\Scripts\Pipeline Scripts Shared Logic\
```

It is imported by `sing_pipeline_v2.py` and all three standalone scripts. It contains:

- `genotype_to_symbol` — converts any raw Climb genotype string to a standard display symbol (`+/-`, `-/-`, `-/Y`, `+/+`, `Inbred`, `Blank`)
- `combine_sample_numbers` — compresses a list of sample names into a range string (e.g. `['1000-0', '1001-1']` → `'1000-1001'`)
- `natural_sort_key` — sort key for numeric-aware string ordering

Because `sing_common.py` lives in a different folder from the scripts, each script adds that folder to `sys.path` at startup. If you move the files, update the path in the `sys.path.insert` line near the top of each script.

**Two intentional duplications remain** — do not "fix" these:
- `sing_pipeline_v2.py` has its own `genotype_to_symbol` that calls `canonicalize_genotype()`. This is separate from `sing_common`'s regex version by design — they serve different purposes in the pipeline's canonical chain.
- `clean_genotype_base` exists in both `sing_pipeline_v2.py` and `Climb_to_Envision_Translation.py` with intentionally different behaviour (the Envision version strips zygosity word tokens; the pipeline version does not).

---

## Running the Pipeline

Double-click `sing_pipeline_v2.py`, or run from the command line:

```bash
python sing_pipeline_v2.py
```

The GUI walks through four screens:

1. **File Setup** — select input files (auto-detected from the script folder)
2. **Wednesday Capacity** — enter how many behavior slots are already booked per Wednesday
3. **Harvest Assignment Review** — review and adjust harvest type per animal, then confirm
4. **Running** — pipeline executes with live log output
5. **Summary** — output files listed with file sizes

---

## Input Files

Place these in the same folder as the script before running:

| File | Description |
|---|---|
| `animals.csv` | Alive animal export from Climb |
| `Sing Harvest Sheet - Summary Sheet.csv` | Tracking sheet with completed harvest counts per strain |
| `births.csv` | Birth records export from Climb |
| `harvest_overrides.csv` | Auto-generated after each run — shows confirmed assignments for reference |

---

## Output Files

All outputs are saved to the same directory as the script, timestamped:

| Output | Description |
|---|---|
| `Complete_Schedule_*.xlsx` | Master schedule with tabs: Summary, P14 Schedule, P56 Schedule, Genotype Excluded Details, All Animals |
| `Harvest_Sheet_Import_*.xlsx` | Per-date harvest sheets for use at the bench |
| `Climb_Sample_Import_*.csv` | Sample import file for Climb |
| `Lab_Data_Export_*.xlsx` | Deliverables tracker (Animal and Sample Tracking, MERFISH Sample Tracker, RNASeq Sample Tracker) |
| `Envision_*.xlsx` | Climb-to-Envision tag import file (NB animals excluded) |
| `Labels_Mailmerge_*.xlsx` | Printable sample labels |
| `harvest_overrides.csv` | Confirmed harvest type assignments from the GUI review |
| `logs/scheduler_*.log` | Full run log with diagnostics |

---

## Harvest Assignment Review

### Pass 1
The review screen shows every P14 and P56 assigned animal with its auto-suggested harvest type. You can:

- Change any animal's harvest type using the dropdown
- Mark animals as **Do Not Schedule (DNS)** to exclude them from this run
- Mark animals as **Extra** to include them as cage-fillers
- Assign **NB types** (Perfusion NB, MERFISH NB, RNAseq NB) for animals without a behavior session
- Use **Reset to Suggested** to revert all changes
- Use **Skip / Use Auto-Assignments** to bypass the review

The **Group** column shows group size per P56 Wednesday session:
- `✓ 3` — complete group of 3
- `⚠ 2` — incomplete group (flagged for NB consideration)

### Pass 2 (automatic, if DNS animals exist)
If any animals are marked DNS, a second GUI appears automatically showing all animals with:
- Pass 1 choices pre-filled for all non-DNS animals
- DNS animals re-offered blank for potential NB assignment
- All choices still editable

### NB Harvest Types
NB (No Behavior) types are for adult animals that cannot complete a full behavior group but may still be useful for harvest:
- Same protocols as regular types
- Envision Date shows `NB` instead of a behavior date
- **Excluded from Envision output** — no tagging session needed
- Still appear in Harvest Sheet and Labels

### Row colors
Indicate the current harvest type:
- Green = Perfusion, Blue = MERFISH, Yellow = RNAseq
- Purple = Extra, Darker shades = NB variants, Red = Do Not Schedule

---

## Key Scheduling Logic

- **P14 harvest**: Birth date + 14 days. Must fall Mon–Fri. Individual animals — no group requirement.
- **P56 behavior**: First Wednesday falling in the P42–P49 window (age in days).
- **P56 harvest**: Behavior date + 14 days (always exactly 2 weeks later).
- **Envision tagging**: Always exactly 2 weeks before the harvest date.
- **Capacity**: Wednesday behavior sessions capped at 18 animals (`WEDNESDAY_CAPACITY` in CONFIG).
- **Minimum group size**: P56 animals must be in groups of 3 to be scheduled. Incomplete groups go to Unschedulable unless assigned an NB type.
- **Toe clip animals**: Excluded from P56/behavior (gait effects). Ear-notched animals used for P56.
- **All strains**: Weighed equally against tracking sheet quota — no strains are blocked from P56 scheduling.

### Quota and Flex Slot
- Target: 5 Male + 5 Female Perfusion per strain per timepoint
- **+1 flex slot**: Once either sex hits its quota, one additional animal of that sex is allowed (the 11th animal)
- The flex slot is tracked across runs — if already used (completed > target + 1), no further overages allowed
- MERFISH and RNAseq quotas are tracked separately (1 per sex per timepoint)

### SHANK3 Split Quotas
SHANK3-Het and SHANK3-Hom are tracked as separate strains in the tracking sheet and pipeline. The pipeline tries `STRAIN-HET` / `STRAIN-HOM` keys first, falling back to `STRAIN` for all other strains.

### All Animals Tab
The All Animals tab in Complete_Schedule.xlsx contains every animal from the input file with:
- Scheduling result columns (Assigned_Timepoint, Harvest_Type, Assignment_Reason) joined from pipeline results
- `P14_Date` shows `Too Old` for animals that missed the P14 window
- `Assigned_Timepoint` always filled: P14, P56, or Unschedulable
- `Harvest_Type` always filled: harvest type or N/A
- `Assignment_Reason` always filled with a specific explanation

---

## Configuration

All tunable parameters live in the `CONFIG` dict near the top of the script. Key settings:

```python
'WEDNESDAY_CAPACITY': 18,       # Max animals per behavior Wednesday
'CAGE_SIZE': 3,                 # Animals per cage (minimum group size)
'P14_VALID_DAYS': [0,1,2,3,4], # Mon–Fri
'HARVEST_TARGETS': {            # Per-strain per-sex targets
    'Perfusion': 5,
    'MERFISH': 1,
    'RNAseq': 1
},
```

---

## Version History

### v2.2 — 2026-04-21 (`sing_pipeline_v2.py`)

**Shared utilities**
- Extracted `genotype_to_symbol`, `combine_sample_numbers`, and `natural_sort_key` into `sing_common.py`
- All three standalone scripts (`Label_generator.py`, `Deliverables_Sheet_Export.py`, `Climb_to_Envision_Translation.py`) now import from `sing_common.py` — one fix propagates everywhere
- Removed duplicate `get_starting_sample_number` definition in `sing_pipeline_v2.py` (terminal version at ~line 7308 was shadowed by GUI version; terminal version deleted)

**Code quality**
- Replaced all 18 bare `except:` clauses with `except Exception:` across all four files — `KeyboardInterrupt` and `SystemExit` no longer silently swallowed

**Deliverables output**
- Genotype column in all three tracker sheets now shows `"<Line Short> <symbol>"` (e.g. `"Mecp2 +/-"`, `"Shank3 -/-"`, `"B6J Inbred"`) instead of bare symbol

**Scheduling**
- Removed P56 behavior-complete strain list — all strains now weighed equally against tracking sheet quota

### v2.1 — 2026-04-08 (`sing_pipeline_v2.py`)

**Harvest Review GUI**
- Added NB harvest types (Perfusion NB, MERFISH NB, RNAseq NB) for animals without behavior
- Added Group column showing group size per P56 session (✓ 3 or ⚠ N)
- Two-pass review: Pass 2 automatically launches when DNS animals exist, with Pass 1 choices pre-filled
- DNS animals re-offered in Pass 2 for potential NB assignment
- Incomplete P56 groups (< 3 animals) shown with NB suggestion for non-B6 strains
- B6/B6NJ incomplete groups go to Unschedulable (always need groups of 3)
- Extra animals always included in P56 schedule

**Quota / Scheduling Logic**
- P14 quota limit: stops scheduling once per-sex quota is met
- Flex slot (+1): fires as soon as one sex hits its quota; blocked if already used in previous run (total completed ≥ target×2+1)
- Composite strain key: SHANK3-Het and SHANK3-Hom tracked as separate quotas
- Auto-type suggestions: over-quota animals now correctly suggest Extra or NB
- Incomplete groups (non-B6) go straight to Unschedulable
- B6 monthly minimum enforcement disabled — managed manually

**Outputs**
- All Animals tab: all animals from input file, scheduling data merged in, Assignment_Reason always populated, P14_Date shows "Too Old" where applicable
- Complete_Schedule.xlsx: removed Wednesday Capacity, Requirements Status, Strain Summary tabs — now shows only Summary, P14, P56, Genotype Excluded, All Animals
- Lab_Data_Export.xlsx: removed Sing Harvest Sheet tab (use Harvest_Sheet_Import file instead)
- NB animals excluded from Envision output; Envision Date shows "NB"
- Climb Sample Import saved as .csv

**Code**
- `resolve_strain_key()` helper for composite strain+genotype quota lookup
- `harvest_overrides.csv` always overwritten after GUI confirmation
- Genotype canonicalization: HET1, HET2, HOM1, HOM2 patterns now correctly recognized

### v2.0 (`sing_pipeline_v2.py`)
Harvest review selection capture fixed, GUI improvements, output format fixes, Python Expert refactor.

### v1.7 (`sing_pipeline.py`)
Previous production version — kept as fallback.

---

## Project Context

SING spans three institutions — JAX (Kumar Lab), Penn State (Paul Lab), and NYU — and tracks active strains out of 114 total over the project lifetime. Animals are housed in rooms B6 and F29.

**Key collaborators:** Marina Santos (OFA behavior), Tuan Nguyen (data analysis), Fionna Kennedy (Envision tagging and animal entry), Sean Deats (harvester).

---

*Questions or issues: contact Tim Leach, Kumar Lab, JAX.*
