# =============================================================================
# Breeding Rotation Tracker
# Version: 1.0.0
# Updated: 2026-06-04
#
# Standalone colony rotation tool for SING breeding management.
# Shows monthly rotation status, NP flags, missing sire/dam alerts,
# and breeding candidate suggestions from animals.csv.
#
# Usage: double-click Breeding_Rotation_Tracker.bat
# Inputs: matings.csv  (Climb active matings export)
#         births.csv   (Climb births export — same file as pipeline)
#         animals.csv  (optional — enables candidate lookup)
# =============================================================================

import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
import os
import re
from datetime import date
from math import ceil

# ── Configuration ─────────────────────────────────────────────────────────────
COLONY_ROTATION_DAYS     = 180
NP_NO_BIRTHS_DAYS        = 90
NP_GONE_QUIET_DAYS       = 60
BREEDING_AGE_MIN_DAYS    = 56
BREEDING_AGE_MAX_DAYS    = 84

COMPLETING_STRAINS = [
    'B6.129(FVB)-Cdkl5<tm1.1Joez>/J',
    'B6.129S4-C3<tm1Crr>/J',
    'B6J-Cntnap2-/-',
    'B6J-Fmr1 -/- (X chr)',
    'B6NJ-Kcnd3-/- Cyfip2-S968F<J> Hom Breed Well',
]

# Climb full Line name → Line (Short) in animals.csv
CLIMB_TO_SHORT = {
    'B6.129-Shank3<tm2Gfng>/J':                                          'SHANK3',
    'B6NJ-Kcnd3-/- Cyfip2-S968F<J> Hom Breed Well':                      'KCND3',
    'B6J-Fmr1 -/- (X chr)':                                              'FMR1',
    'B6.129(FVB)-Cdkl5<tm1.1Joez>/J':                                    'CDKL5',
    'B6J-Cntnap2-/-':                                                     'CNTNAP2',
    'B6.129S4-C3<tm1Crr>/J':                                              'C3',
    'B6NJ-Bcl11b Cyfip2-S968F<J> H Lethal':                              'BCL11B',
    'B6NJ-Cyfip2-S968F<J> (GET204)':                                      'GET204',
    'C57BL/6J':                                                           'B6J',
    'C57BL/6NJ':                                                          'B6NJ',
    '(C57BL/6J x 129S1/SvImJ-Scn1a<em1Dsf>/J)F1 - Affected':            'Dravet',
    '129S1/SvImJ-Scn1a<em1Dsf>/J - Unaffected':                          'Dravet',
    '129S1/SvImJ-Scn1a<em1Dsf>/J - Unaffected ':                         'Dravet',
}

# ── Design tokens ──────────────────────────────────────────────────────────────
_T = {
    'bg':         '#ffffff', 'bg_subtle':  '#f7f8f9', 'bg_inset':   '#f1f3f5',
    'text':       '#111827', 'text_muted': '#6b7280', 'text_faint': '#9ca3af',
    'border':     '#e5e7eb', 'border_mid': '#d1d5db',
    'accent':     '#1D9E75', 'accent_lt':  '#EAF3DE', 'accent_text':'#3B6D11',
    'red':        '#A32D2D', 'red_lt':     '#FCEBEB',
    'amber':      '#854F0B', 'amber_lt':   '#FAEEDA',
    'hdr_bg':     '#f7f8f9', 'hdr_border': '#e5e7eb',
}

# ── Helpers ────────────────────────────────────────────────────────────────────

def _parse_geno_symbol(geno_str: str) -> str:
    s = str(geno_str).strip() if geno_str and str(geno_str).strip().lower() != 'nan' else ''
    if not s:
        return 'WT'
    m = re.search(r'([-+*/]/[-+*YyWw])', s)
    return m.group(1) if m else 'WT'


def _monthly_pattern(N: int, cycle: int = 6) -> list:
    """Bresenham distribution of N retirements across cycle months."""
    pattern, error = [], 0
    for _ in range(cycle):
        error += N
        pattern.append(error // cycle)
        error %= cycle
    return pattern


def load_matings(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, encoding='utf-8-sig', dtype=str)
    df = df[df['Status'] == 'Active Mating'].copy()
    df['Line']        = df['Line'].str.strip()
    df['Mating Date'] = pd.to_datetime(df['Mating Date'], errors='coerce')
    df = df.dropna(subset=['Mating Date'])
    today             = pd.Timestamp(date.today())
    df['days_active'] = (today - df['Mating Date']).dt.days.astype(int)
    df['Births']      = pd.to_numeric(df['Births'], errors='coerce').fillna(0).astype(int)
    df['Comments']    = df['Comments'].fillna('').str.strip()
    df['sire_blank']  = df['Sire(s) Name(s)'].fillna('').str.strip() == ''
    df['dam_blank']   = df['Dam(s) Name(s)'].fillna('').str.strip()  == ''
    return df


def enrich_with_births(matings_df: pd.DataFrame, births_filepath: str) -> pd.DataFrame:
    births = pd.read_csv(births_filepath, encoding='utf-8-sig', dtype=str)
    births['Birth Date'] = pd.to_datetime(births['Birth Date'], errors='coerce')
    births['Live Count'] = pd.to_numeric(births['Live Count'], errors='coerce').fillna(0).astype(int)
    last_litter = births.groupby('Mating ID')['Birth Date'].max().rename('last_litter_date')
    total_live  = births.groupby('Mating ID')['Live Count'].sum().rename('live_births')
    df = matings_df.join(last_litter, on='Mating ID').join(total_live, on='Mating ID')
    df['live_births'] = df['live_births'].fillna(0).astype(int)
    today = pd.Timestamp(date.today())
    df['days_since_litter'] = (today - df['last_litter_date']).dt.days
    df['np_zero']  = (df['days_active'] >= NP_NO_BIRTHS_DAYS)    & (df['live_births'] == 0)
    df['np_quiet'] = (df['live_births'] > 0) & (df['days_since_litter'] >= NP_GONE_QUIET_DAYS)
    df['is_np']    = df['np_zero'] | df['np_quiet']
    return df


def build_analysis(matings_df: pd.DataFrame) -> list:
    completing = set(COMPLETING_STRAINS)
    today      = pd.Timestamp(date.today())
    results    = []

    for line, grp in matings_df.groupby('Line'):
        grp  = grp.sort_values('Mating Date').copy()
        N    = len(grp)
        interval_mo    = max(1, ceil(6 / N))
        pat            = _monthly_pattern(N)
        newest_date    = grp['Mating Date'].max()
        days_since_new = int((today - newest_date).days)
        swap_due       = (days_since_new / 30) >= interval_mo

        np_units      = grp[grp['is_np']]
        overdue_units = grp[grp['days_active'] >= COLONY_ROTATION_DAYS]
        missing_units = grp[grp['sire_blank'] | grp['dam_blank']]

        non_np = grp[~grp['is_np']].sort_values(
            ['live_births', 'days_active'], ascending=[True, False])
        cadence_candidate = non_np.iloc[0] if (swap_due and not non_np.empty) else None

        sire_geno = dam_geno = ''
        for _, r in grp.iterrows():
            sg = str(r.get("Sire(s) Genotype(s)", '') or '').strip()
            dg = str(r.get("Dam(s) Genotype(s)",  '') or '').strip()
            if not r['sire_blank'] and not sire_geno and sg:
                sire_geno = sg
            if not r['dam_blank'] and not dam_geno and dg:
                dam_geno = dg.split(',')[0].strip()

        results.append({
            'line': line, 'N': N,
            'interval_months': interval_mo,
            'monthly_pattern': pat,
            'is_completing':   line in completing,
            'swap_due':        swap_due,
            'days_since_newest': days_since_new,
            'next_swap_date':  newest_date + pd.DateOffset(months=interval_mo),
            'np_units':        np_units,
            'overdue_units':   overdue_units,
            'missing_units':   missing_units,
            'cadence_candidate': cadence_candidate,
            'sire_geno':       sire_geno,
            'dam_geno':        dam_geno,
            'all_units':       grp,
        })

    results.sort(key=lambda x: (
        x['is_completing'],
        -(len(x['np_units']) + len(x['overdue_units']) + len(x['missing_units']))
    ))
    return results


def find_candidates(animals_df, line: str, sire_geno: str, dam_geno: str) -> dict:
    short = CLIMB_TO_SHORT.get(line.strip())
    empty = {'males': [], 'females': [], 'line_short': short, 'sufficient': False}
    if not short or animals_df is None or animals_df.empty:
        return empty
    today  = pd.Timestamp(date.today())
    ls_col = animals_df.get('Line (Short)', pd.Series(dtype=str)).str.strip()
    strain = animals_df[ls_col == short].copy()
    if strain.empty:
        return empty
    strain['_bd']  = pd.to_datetime(strain['Birth Date'], errors='coerce')
    strain         = strain.dropna(subset=['_bd'])
    strain['_age'] = (today - strain['_bd']).dt.days
    strain         = strain[(strain['_age'] >= BREEDING_AGE_MIN_DAYS) &
                            (strain['_age'] <= BREEDING_AGE_MAX_DAYS)]
    if strain.empty:
        return empty
    req_sire = _parse_geno_symbol(sire_geno)
    req_dam  = _parse_geno_symbol(dam_geno)

    def _matches(geno, required):
        if required == 'WT':
            return _parse_geno_symbol(geno) in ('+/+', 'WT')
        return _parse_geno_symbol(geno) == required

    males   = strain[strain['Sex'].str.strip().str.upper() == 'M'].copy()
    females = strain[strain['Sex'].str.strip().str.upper() == 'F'].copy()
    if req_sire != 'WT':
        males = males[males['Genotype'].apply(lambda g: _matches(str(g), req_sire))]
    if req_dam != 'WT':
        females = females[females['Genotype'].apply(lambda g: _matches(str(g), req_dam))]

    def _fmt(row):
        return {'name': row['Name'], 'age_days': int(row['_age']),
                'genotype': str(row.get('Genotype', ''))}
    return {
        'males':      [_fmt(r) for _, r in males.iterrows()],
        'females':    [_fmt(r) for _, r in females.iterrows()],
        'line_short': short,
        'sufficient': len(males) >= 1 and len(females) >= 1,
    }


# ── GUI ────────────────────────────────────────────────────────────────────────

def run_gui():
    root = tk.Tk()
    root.title('SING Breeding Rotation Tracker')
    root.configure(bg=_T['bg'])
    root.resizable(True, True)

    script_dir = os.path.dirname(os.path.abspath(__file__))

    # ── detect default file paths ─────────────────────────────────────────────
    default_matings = os.path.join(script_dir, 'matings.csv')
    default_births  = os.path.join(script_dir, 'births.csv')
    default_animals = os.path.join(script_dir, 'animals.csv')

    # ── shared helpers ────────────────────────────────────────────────────────
    def _make_header(title, subtitle=''):
        hdr = tk.Frame(root, bg=_T['hdr_bg'], pady=16)
        hdr.pack(fill='x')
        tk.Frame(root, bg=_T['hdr_border'], height=1).pack(fill='x')
        tk.Label(hdr, text='SING BREEDING ROTATION TRACKER',
                 font=('Helvetica', 9), bg=_T['hdr_bg'],
                 fg=_T['text_faint']).pack()
        tk.Label(hdr, text=title,
                 font=('Helvetica', 16, 'bold'),
                 bg=_T['hdr_bg'], fg=_T['text']).pack()
        if subtitle:
            tk.Label(hdr, text=subtitle, font=('Helvetica', 10),
                     bg=_T['hdr_bg'], fg=_T['text_muted']).pack(pady=(2, 0))

    def _make_footer():
        tk.Frame(root, bg=_T['border'], height=1).pack(fill='x')
        foot = tk.Frame(root, bg=_T['bg_subtle'], pady=10)
        foot.pack(fill='x')
        return foot

    def _btn(parent, text, command, style='primary'):
        styles = {
            'primary':   ('#1D9E75', '#ffffff', '#15825f'),
            'secondary': (_T['bg_subtle'], _T['text'], _T['bg_inset']),
            'ghost':     (_T['bg'], _T['text_muted'], _T['bg_subtle']),
        }
        bg, fg, ab = styles.get(style, styles['secondary'])
        return tk.Button(parent, text=text, command=command,
                         bg=bg, fg=fg, activebackground=ab,
                         font=('Helvetica', 10, 'bold' if style == 'primary' else 'normal'),
                         relief='flat', bd=0, padx=16, pady=7, cursor='hand2')

    def _switch(fn):
        for w in root.winfo_children():
            w.destroy()
        fn()

    # ── Screen 1: File Setup ──────────────────────────────────────────────────
    def screen_files():
        root.title('Breeding Rotation Tracker — Load Files')
        root.geometry('640x420')

        _make_header('Colony Rotation Tracker',
                     'Load your Climb exports to review breeding status.')

        body = tk.Frame(root, bg=_T['bg'], padx=28, pady=20)
        body.pack(fill='both', expand=True)

        files = [
            ('matings.csv',  'Climb active matings export', default_matings,  True),
            ('births.csv',   'Climb births export',         default_births,   True),
            ('animals.csv',  'Optional — enables candidate animal lookup',
             default_animals, False),
        ]

        path_vars   = {}
        toggle_vars = {}
        status_lbls = {}

        def _update(key, var, lbl):
            p = var.get().strip()
            if not p:
                lbl.configure(text='', fg=_T['text_faint'])
            elif os.path.exists(p):
                lbl.configure(text='✓ Found', fg=_T['accent'])
            else:
                lbl.configure(text='✗ Not found', fg=_T['red'])

        def _browse(key, var, lbl, title):
            p = filedialog.askopenfilename(
                parent=root, title=title,
                initialdir=os.path.dirname(var.get()) or script_dir,
                filetypes=[('CSV files', '*.csv'), ('All files', '*.*')])
            if p:
                var.set(p)
                toggle_vars[key].set(True)
                _update(key, var, lbl)

        for key, desc, default, required in files:
            row = tk.Frame(body, bg=_T['bg'])
            row.pack(fill='x', pady=6)

            tvar = tk.BooleanVar(value=os.path.exists(default))
            pvar = tk.StringVar(value=default)
            toggle_vars[key] = tvar
            path_vars[key]   = pvar

            tk.Checkbutton(row, variable=tvar, bg=_T['bg'],
                           activebackground=_T['bg']).pack(side='left')
            lbl_col = tk.Frame(row, bg=_T['bg'])
            lbl_col.pack(side='left', fill='x', expand=True, padx=(4, 0))
            tk.Label(lbl_col, text=key + (' *' if required else ''),
                     font=('Helvetica', 10, 'bold'), bg=_T['bg'],
                     fg=_T['text']).pack(anchor='w')
            tk.Label(lbl_col, text=desc, font=('Helvetica', 9),
                     bg=_T['bg'], fg=_T['text_muted']).pack(anchor='w')

            right = tk.Frame(row, bg=_T['bg'])
            right.pack(side='right')
            sl = tk.Label(right, text='', font=('Helvetica', 9),
                          bg=_T['bg'], width=12)
            sl.pack(side='right')
            status_lbls[key] = sl
            _update(key, pvar, sl)
            pvar.trace_add('write', lambda *_, k=key, v=pvar, l=sl: _update(k, v, l))
            _btn(right, 'Browse', lambda k=key, v=pvar, l=sl:
                 _browse(k, v, l, f'Select {k}'),
                 style='secondary').pack(side='right', padx=(0, 6))

        err_lbl = tk.Label(body, text='', font=('Helvetica', 9, 'italic'),
                           bg=_T['bg'], fg=_T['red'])
        err_lbl.pack(pady=(8, 0))

        foot = _make_footer()

        def _load():
            err_lbl.configure(text='')
            m_path = path_vars['matings.csv'].get().strip()
            b_path = path_vars['births.csv'].get().strip()
            a_path = path_vars['animals.csv'].get().strip()

            if not toggle_vars['matings.csv'].get() or not os.path.exists(m_path):
                err_lbl.configure(text='⚠  matings.csv is required.')
                return
            if not toggle_vars['births.csv'].get() or not os.path.exists(b_path):
                err_lbl.configure(text='⚠  births.csv is required.')
                return

            try:
                matings_df = load_matings(m_path)
                matings_df = enrich_with_births(matings_df, b_path)
            except Exception as ex:
                err_lbl.configure(text=f'⚠  Error loading files: {ex}')
                return

            animals_df = None
            if toggle_vars['animals.csv'].get() and os.path.exists(a_path):
                try:
                    animals_df = pd.read_csv(a_path, dtype=str, encoding='utf-8-sig')
                except Exception:
                    pass

            analysis = build_analysis(matings_df)
            _switch(lambda: screen_rotation(analysis, animals_df))

        _btn(foot, 'Load & Analyse  →', _load).pack(side='right', padx=16)

    # ── Screen 2: Rotation Analysis ───────────────────────────────────────────
    def screen_rotation(analysis, animals_df):
        root.title('Breeding Rotation Tracker — Colony Status')
        root.geometry('980x720')

        n_action = sum(1 for s in analysis
                       if len(s['np_units']) + len(s['overdue_units']) +
                          len(s['missing_units']) > 0 or s['swap_due'])
        _make_header(
            'Colony Rotation Status',
            f'{len(analysis)} strains  •  {n_action} need attention  •  '
            f'{date.today().strftime("%B %d, %Y")}',
        )

        body = tk.Frame(root, bg=_T['bg'])
        body.pack(fill='both', expand=True)

        canvas = tk.Canvas(body, bg=_T['bg'], highlightthickness=0)
        vsb    = ttk.Scrollbar(body, orient='vertical', command=canvas.yview)
        canvas.configure(yscrollcommand=vsb.set)
        vsb.pack(side='right', fill='y')
        canvas.pack(side='left', fill='both', expand=True)

        inner  = tk.Frame(canvas, bg=_T['bg'])
        win_id = canvas.create_window((0, 0), window=inner, anchor='nw')
        inner.bind('<Configure>',
                   lambda e: canvas.configure(scrollregion=canvas.bbox('all')))
        canvas.bind('<Configure>',
                    lambda e: canvas.itemconfig(win_id, width=e.width))

        def _wheel(e):
            canvas.yview_scroll(int(-1 * (e.delta / 120)), 'units')
        canvas.bind_all('<MouseWheel>', _wheel)

        # ── Render each strain card ───────────────────────────────────────────
        for s in analysis:
            line    = s['line']
            has_np  = len(s['np_units'])      > 0
            has_mis = len(s['missing_units']) > 0
            has_ov  = len(s['overdue_units']) > 0
            is_done = s['is_completing']

            if has_np or has_mis:
                bg, border = _T['red_lt'],    _T['red']
            elif has_ov or s['swap_due']:
                bg, border = _T['amber_lt'],  _T['amber']
            elif is_done:
                bg, border = _T['bg_inset'],  _T['border_mid']
            else:
                bg, border = _T['accent_lt'], _T['accent']

            card = tk.Frame(inner, bg=bg, padx=14, pady=10,
                            highlightbackground=border, highlightthickness=1)
            card.pack(fill='x', padx=10, pady=3)

            # Header row
            hrow = tk.Frame(card, bg=bg)
            hrow.pack(fill='x')
            short  = CLIMB_TO_SHORT.get(line.strip(), line[:35])
            tk.Label(hrow, text=short, font=('Helvetica', 12, 'bold'),
                     bg=bg, fg=_T['text']).pack(side='left')
            N      = s['N']
            pat_str = ','.join(str(x) for x in s['monthly_pattern'])
            suffix = ('  [COMPLETING]' if is_done
                      else f'   {N} unit{"s" if N != 1 else ""}  '
                           f'\u2022  {pat_str} /mo')
            tk.Label(hrow, text=suffix, font=('Helvetica', 9),
                     bg=bg, fg=_T['text_muted']).pack(side='left', padx=4)

            # On-track shortcut
            if not (has_np or has_mis or has_ov or s['swap_due']):
                nxt = s['next_swap_date'].strftime('%b %d')
                tk.Label(card,
                         text=f'\u2705  On track  \u2014  next swap due {nxt}',
                         font=('Helvetica', 9), bg=bg,
                         fg=_T['accent_text']).pack(anchor='w', pady=(2, 0))
                continue

            # Units table
            tbl = tk.Frame(card, bg=bg)
            tbl.pack(fill='x', pady=(6, 2))
            for ci, (txt, w) in enumerate([
                ('Housing', 7), ('Mating', 7), ('Days\nActive', 6),
                ('Live\nBirths', 7), ('Last\nLitter', 11), ('Status', 30)
            ]):
                tk.Label(tbl, text=txt, width=w, anchor='center',
                         font=('Helvetica', 8), bg=bg,
                         fg=_T['text_faint']).grid(row=0, column=ci, padx=2)

            for ri, (_, r) in enumerate(s['all_units'].iterrows(), 1):
                flags = []
                if r.get('np_zero'):
                    flags.append('NP: no births 90d+')
                if r.get('np_quiet'):
                    flags.append(f'NP: quiet {int(r.get("days_since_litter", 0))}d')
                if r['days_active'] >= COLONY_ROTATION_DAYS:
                    flags.append(f'Overdue {r["days_active"]}d')
                if r.get('sire_blank'):
                    flags.append('\u26a0 No sire logged')
                if r.get('dam_blank'):
                    flags.append('\u26a0 No dam(s) logged')
                is_cad = (s['cadence_candidate'] is not None
                          and r['Housing ID'] == s['cadence_candidate']['Housing ID']
                          and not flags)
                if is_cad:
                    flags.append('\u2192 Retire next (cadence)')
                ll = (r['last_litter_date'].strftime('%b %d')
                      if pd.notna(r.get('last_litter_date')) else '\u2014')
                row_bg = (_T['red_lt'] if any('NP' in f or 'No sire' in f or
                                               'No dam' in f for f in flags)
                          else _T['amber_lt'] if flags else bg)
                fg_c   = _T['red'] if row_bg == _T['red_lt'] else _T['text']
                vals   = [r['Housing ID'], r['Mating ID'], str(r['days_active']),
                          str(r.get('live_births', 0)), ll,
                          '  '.join(flags) or 'OK']
                for ci, (v, w) in enumerate(zip(vals, [7, 7, 6, 7, 11, 30])):
                    tk.Label(tbl, text=v, width=w, anchor='center',
                             font=('Helvetica', 9), bg=row_bg,
                             fg=fg_c).grid(row=ri, column=ci, padx=2, pady=1)

            # Replacement candidates
            if not is_done and (s['swap_due'] or has_np):
                cands   = find_candidates(animals_df, line,
                                          s['sire_geno'], s['dam_geno'])
                comment = (s['all_units'].iloc[0]['Comments']
                           if not s['all_units'].empty else '')
                tk.Label(card,
                         text=f'SET UP REPLACEMENT   [{comment or "see matings"}]',
                         font=('Helvetica', 9, 'bold'), bg=bg,
                         fg=_T['text']).pack(anchor='w', pady=(8, 0))

                if not cands.get('line_short'):
                    tk.Label(card,
                             text='\u26a0  Line (Short) mapping unknown \u2014 update CLIMB_TO_SHORT',
                             font=('Helvetica', 9, 'italic'), bg=bg,
                             fg=_T['amber']).pack(anchor='w')
                elif not cands.get('sufficient'):
                    nm = len(cands.get('males',   []))
                    nf = len(cands.get('females', []))
                    tk.Label(card,
                             text=(f'  No sufficient candidates  '
                                   f'({nm} male{"s" if nm != 1 else ""},  '
                                   f'{nf} female{"s" if nf != 1 else ""}  '
                                   f'at {BREEDING_AGE_MIN_DAYS}\u2013'
                                   f'{BREEDING_AGE_MAX_DAYS}d)'),
                             font=('Helvetica', 9, 'italic'), bg=bg,
                             fg=_T['amber']).pack(anchor='w')
                else:
                    sire_sym = _parse_geno_symbol(s['sire_geno'])
                    dam_sym  = _parse_geno_symbol(s['dam_geno'])
                    cgrid    = tk.Frame(card, bg=bg)
                    cgrid.pack(fill='x', pady=(4, 0))
                    for ci, (sex_lbl, lst) in enumerate([
                        (f'Males  ({sire_sym})',  cands['males']),
                        (f'Females  ({dam_sym})', cands['females']),
                    ]):
                        col_f = tk.Frame(cgrid, bg=bg, padx=4)
                        col_f.grid(row=0, column=ci, sticky='nw', padx=(0, 20))
                        tk.Label(col_f, text=sex_lbl,
                                 font=('Helvetica', 9, 'bold'), bg=bg,
                                 fg=_T['text_muted']).pack(anchor='w')
                        for animal in lst[:8]:
                            lbl = (f"  {animal['name']}  "
                                   f"({animal['age_days']}d)  "
                                   f"{animal['genotype']}")
                            tk.Label(col_f, text=lbl, font=('Helvetica', 9),
                                     bg=bg, fg=_T['text']).pack(anchor='w')

            # Missing sire/dam advisory
            for _, mr in s['missing_units'].iterrows():
                parts = []
                if mr.get('sire_blank'): parts.append('sire')
                if mr.get('dam_blank'):  parts.append('dam(s)')
                who = ' & '.join(parts)
                tk.Label(card,
                         text=(f'\u26a0  H{mr["Housing ID"]}  M{mr["Mating ID"]}'
                               f'  \u2014  no {who} logged  '
                               f'(touring male, death, or other)  \u2014  '
                               f'replace {who} individually or retire unit.'),
                         font=('Helvetica', 9), bg=bg, fg=_T['red'],
                         wraplength=880, justify='left',
                         ).pack(anchor='w', pady=(4, 0))

        if not analysis:
            tk.Label(inner, text='No active matings found.',
                     font=('Helvetica', 11), bg=_T['bg'],
                     fg=_T['text_muted']).pack(pady=40)

        # Footer
        foot = _make_footer()

        _btn(foot, '\u2190 Back',
             lambda: [canvas.unbind_all('<MouseWheel>'), _switch(screen_files)],
             style='ghost').pack(side='left', padx=16)

        def _export():
            path = filedialog.asksaveasfilename(
                parent=root, title='Save Rotation Report',
                defaultextension='.txt',
                filetypes=[('Text files', '*.txt'), ('All files', '*.*')],
                initialfile=f'Breeding_Rotation_{date.today()}.txt',
            )
            if not path:
                return
            lines_out = [
                f'SING Breeding Rotation Report',
                f'Generated: {date.today().strftime("%B %d, %Y")}',
                f'',
            ]
            for s in analysis:
                short   = CLIMB_TO_SHORT.get(s['line'].strip(), s['line'])
                pat_str = ','.join(str(x) for x in s['monthly_pattern'])
                tag     = ' [COMPLETING]' if s['is_completing'] else ''
                lines_out.append(f'{"="*60}')
                lines_out.append(f'{short}{tag}  —  N={s["N"]}  pattern={pat_str}/mo')
                for _, r in s['all_units'].iterrows():
                    fl = []
                    if r.get('np_zero'):   fl.append('NP-no-births')
                    if r.get('np_quiet'):  fl.append('NP-quiet')
                    if r['days_active'] >= COLONY_ROTATION_DAYS: fl.append('OVERDUE')
                    if r.get('sire_blank'): fl.append('no-sire')
                    if r.get('dam_blank'):  fl.append('no-dam')
                    ll = (r['last_litter_date'].strftime('%Y-%m-%d')
                          if pd.notna(r.get('last_litter_date')) else 'never')
                    lines_out.append(
                        f"  H{r['Housing ID']:>5}  M{r['Mating ID']:>5}  "
                        f"{r['days_active']:>3}d  {r.get('live_births',0):>3} live  "
                        f"last:{ll}  {'  '.join(fl) or 'OK'}"
                    )
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines_out))
                messagebox.showinfo('Saved', f'Report saved to:\n{path}')
            except Exception as ex:
                messagebox.showerror('Save Error', str(ex))

        _btn(foot, 'Export Report', _export,
             style='secondary').pack(side='right', padx=(0, 8))
        _btn(foot, 'Close', root.destroy,
             style='secondary').pack(side='right')

    # ── Start ─────────────────────────────────────────────────────────────────
    screen_files()
    root.mainloop()


if __name__ == '__main__':
    run_gui()
