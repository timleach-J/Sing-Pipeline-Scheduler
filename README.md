# SING Pipeline Scheduler

**Kumar Lab — The Jackson Laboratory**
Scalable and Systematic Neurobiology of NPD Risk Genes (SING)
NIH-funded, 2024–2029

---

## Overview

The SING Pipeline Scheduler is a Python/tkinter GUI tool that automates harvest scheduling, Envision tagging prep, sample tracking, and output file generation for the SING project. It processes animal colony data exported from Climb/Envision and produces a complete set of Excel files ready for the harvest workflow.

Built and maintained by Tim Leach, Research Technician, Kumar Lab.

---

## Files

| File | Version | Status | Notes |
|---|---|---|---|
| `sing_pipeline.py` | v1.6 | Stable | Production version — use this if v2.0 has issues |
| `sing_pipeline_v2.py` | v2.0 | Current | Python Expert refactor — correctness, performance, documentation |

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

## Input Files

Place these in the same folder as the script before running:

| File | Description |
|---|---|
| `animals.csv` | Alive animal export from Climb |
| `Sing Harvest Sheet - Summary Sheet.csv` | Tracking sheet with completed harvest counts per strain |
| `births.csv` | Birth records export from Climb |
| `harvest_overrides.csv` | *(Optional)* Manual harvest date overrides — leave rows blank for auto-assignment |

---

## Running the Pipeline

Double-click either `.py` file, or run from the command line:

```bash
python sing_pipeline_v2.py
```

The GUI walks through four screens:

1. **File Setup** — select input files
2. **Harvest Type** — choose Perfusion, MERFISH-OCT, or RNA-Seq
3. **Running** — pipeline executes with live log output
4. **Summary** — output files listed with file sizes

---

## Output Files

All outputs are saved to the same directory as the script, timestamped:

| Output | Description |
|---|---|
| `SING_Schedule_*.xlsx` | Master harvest schedule |
| `Harvest_Sheet_*.xlsx` | Per-date harvest sheets |
| `Samples_*.xlsx` | Sample tracking sheets |
| `Envision_Import_*.xlsx` | Climb-to-Envision tag import file |
| `Labels_*.xlsx` | Printable sample labels |
| `Perfusion_Labels_*.xlsx` | Combined perfusion label sheet (single file, all dates) |
| `logs/scheduler_*.log` | Run log with full diagnostics |

---

## Key Scheduling Logic

- **P14 harvest**: Birth date + 14 days. Must fall Mon–Fri. Has a strict 2-hour collection window — must be scheduled in advance.
- **P56 behavior**: First Wednesday falling in the P42–P49 window (age in days).
- **P56 harvest**: Behavior date + 14 days (always exactly 2 weeks later).
- **Envision tagging**: Always exactly 2 weeks before the harvest date.
- **Capacity**: Wednesday behavior sessions capped at 18 animals (`WEDNESDAY_CAPACITY` in CONFIG).
- **Toe clip animals**: Excluded from P56/behavior (gait effects). Ear-notched animals used for P56.

### P56 Behavior-Complete Strains

These strains have completed behavior and are blocked from new P56 scheduling:

`CDKL5, C3, GRN, FMR1, KCND3, FBN1, SHANK3, CNTNAP2, CACNA1A`

---

## Configuration

All tunable parameters live in the `CONFIG` dict near the top of the script. Key settings:

```python
'WEDNESDAY_CAPACITY': 18,       # Max animals per behavior Wednesday
'CAGE_SIZE': 3,                 # Animals per cage
'P14_VALID_DAYS': [0,1,2,3,4], # Mon–Fri
'HARVEST_TARGETS': {            # Per-strain per-sex targets
    'Perfusion': 5,
    'MERFISH': 1,
    'RNAseq': 1
},
```

---

## Version History

### v2.0 — 2026-03-24 (`sing_pipeline_v2.py`)
Python Expert refactor. No logic changes — all behavioral output is identical to v1.6.

- Narrowed `warnings.filterwarnings` — no longer silences all warnings globally
- Fixed bare `except` clauses in `auto_size_columns` and `_run_again`
- Removed redundant `date as date_type` alias — all type hints now use `date` directly
- Removed unused `timezone` import
- Moved `import unittest` out of top-level imports
- Added `Any` to typing imports
- Documented unused `sex` param in `get_strain_breeding_type`
- Replaced convoluted `argsort` sort with `sort_values(key=...)` in `build_births_sexing_schedule`
- Simplified `process_large_dataset` — removed unnecessary line-count pre-pass
- Vectorized `filter_animals_by_use` excluded record construction (removed `iterrows`)
- Added docstrings to `to_date`, `validate_config_advanced`, `auto_size_columns`, `get_strain_breeding_type`, `process_large_dataset`, `filter_animals_by_use`
- Replaced ~30 `len(df) == 0` checks with `df.empty`

### v1.6 — 2025 (`sing_pipeline.py`)
Production-stable version. See inline comments for full change history.

---

## Project Context

SING spans three institutions — JAX (Kumar Lab), Penn State (Paul Lab), and NYU — and tracks 8 active strains out of 114 total over the project lifetime. Animals are housed in rooms B6 and F29.

**Key collaborators:** Marina Santos (OFA behavior), Tuan Nguyen (data analysis), Fionna Kennedy (Envision tagging and animal entry), Sean Deats (harvester).

---

*Questions or issues: contact Tim Leach, Kumar Lab, JAX.*
