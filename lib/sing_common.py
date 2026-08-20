"""
sing_common.py — Shared utility functions for the SING Pipeline scripts.

Imported by:
    sing_pipeline_v2.py
    Label_generator.py
    Deliverables_Sheet_Export.py
    Climb_to_Envision_Translation.py

Rules:
    - This file has NO imports beyond the Python standard library and pandas/re,
      which are already required by every script that imports it.
    - Do NOT add pipeline-internal functions here (scheduling logic, GUI code,
      CONFIG dict, canonicalize_genotype, etc.).
    - clean_genotype_base is intentionally NOT here — the pipeline and Envision
      scripts have different implementations that are both correct for their
      respective purposes.
    - The pipeline's own genotype_to_symbol (which calls canonicalize_genotype)
      is also intentionally NOT replaced — it stays in sing_pipeline_v2.py.
      This file's genotype_to_symbol is the standalone regex version used only
      by the three standalone scripts.
"""

import re
import pandas as pd


# ── genotype_to_symbol ────────────────────────────────────────────────────────

def genotype_to_symbol(raw) -> str:
    """Convert any raw Climb genotype string to a standard display symbol.

    Returns one of: '+/-', '-/-', '-/Y', '+/+', 'Inbred', 'Blank'

    Used by: Label_generator.py, Deliverables_Sheet_Export.py,
             Climb_to_Envision_Translation.py

    NOTE: sing_pipeline_v2.py has its own genotype_to_symbol that calls
    canonicalize_genotype() instead. That copy is intentionally separate.

    Examples:
        >>> genotype_to_symbol('HET1')
        '+/-'
        >>> genotype_to_symbol('HOM2')
        '-/-'
        >>> genotype_to_symbol('Shank3<tm2Gfng> HET2')
        '+/-'
        >>> genotype_to_symbol('Fmr1<tm1Cgr> -/-')
        '-/-'
        >>> genotype_to_symbol('Mecp2<tm1.1Bird> -/Y')
        '-/Y'
        >>> genotype_to_symbol('+/+')
        '+/+'
        >>> genotype_to_symbol('Inbred')
        'Inbred'
        >>> genotype_to_symbol('')
        'Blank'
        >>> genotype_to_symbol(None)
        'Blank'
        >>> genotype_to_symbol('Pending')
        'Blank'
    """
    if raw is None or (isinstance(raw, float) and raw != raw):
        return 'Blank'
    s = str(raw).strip().lower()
    if not s or s in ('nan', 'none', 'n/a', '-', ''):
        return 'Blank'
    # Strip allele name markers (angle brackets, square brackets)
    s = re.sub(r'[‹<][^›>]*[›>]', '', s)
    s = re.sub(r'\[[^\]]*\]', '', s)
    s = re.sub(r'probe\s*', '', s)
    s = ' '.join(s.split())
    if any(k in s for k in ('inconclusive', 'pending', 'failed', 'no call')):
        return 'Blank'
    if re.search(r'\bhom\d*\b|-/-', s):
        return '-/-'
    if re.search(r'\bhet\d*\b|-/\+|\+/-', s):
        return '+/-'
    if re.search(r'hem[i]?|tg/\+|\+/tg|-/y', s):
        return '-/Y'
    if re.search(r'\+/\+|\bwt\b|wild.?type', s):
        return '+/+'
    if 'inbred' in s:
        return 'Inbred'
    return 'Blank'


# ── combine_sample_numbers ────────────────────────────────────────────────────

def combine_sample_numbers(sample_list) -> str:
    """Combine a list of sample names into a compact range string.

    Strips any suffix (everything after the first '-') before comparing,
    then returns 'min-max' for multiple samples or a bare number for one.

    Examples:
        >>> combine_sample_numbers(['1000-0', '1001-1', '1002-2'])
        '1000-1002'
        >>> combine_sample_numbers(['571'])
        '571'
        >>> combine_sample_numbers([])
        ''
    """
    if not sample_list:
        return ""
    base_numbers = []
    for sample in sample_list:
        sample_str = str(sample)
        base_num = sample_str.split('-')[0] if '-' in sample_str else sample_str
        try:
            base_numbers.append(int(base_num))
        except (ValueError, TypeError):
            continue
    if not base_numbers:
        return ""
    if len(base_numbers) == 1:
        return str(base_numbers[0])
    return f"{min(base_numbers)}-{max(base_numbers)}"


# ── natural_sort_key ──────────────────────────────────────────────────────────

def natural_sort_key(name) -> list:
    """Return a sort key that orders strings containing numbers naturally.

    Splits the string into alternating text and digit runs, converting
    digit runs to integers so that e.g. 'Mouse10' sorts after 'Mouse9'.

    Examples:
        >>> natural_sort_key('Mouse2') < natural_sort_key('Mouse10')
        True
        >>> natural_sort_key('106748') < natural_sort_key('106749')
        True
    """
    if pd.isna(name):
        return []
    parts = re.split(r'(\d+)', str(name))
    return [int(part) if part.isdigit() else part.lower() for part in parts]
