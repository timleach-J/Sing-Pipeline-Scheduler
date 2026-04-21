import pandas as pd
import os
import re
from datetime import datetime
import sys
import traceback
from sing_common import genotype_to_symbol

# ==================== EASY CONFIGURATION ====================
INPUT_SAMPLES_FILE = 'samples.csv'
INPUT_ANIMALS_FILE = 'animals.csv'

LABELS_ACROSS   = 5
LABELS_DOWN     = 17
LABELS_PER_PAGE = LABELS_ACROSS * LABELS_DOWN  # 85
# ============================================================


def load_and_merge_data(samples_file, animals_file):
    print(f"Reading {samples_file}...")
    samples_df = pd.read_csv(samples_file)
    print(f"  Found {len(samples_df)} samples")

    print(f"\nReading {animals_file}...")
    animals_df = pd.read_csv(animals_file)
    print(f"  Found {len(animals_df)} animals")

    print("\nRenaming columns before merge...")

    samples_df = samples_df.rename(columns={
        'Name':         'Sample Name',
        'Source':       'Animal Name',
        'Harvest Date': 'Sample Harvest Date',
    })

    animals_df = animals_df.rename(columns={
        'Name':           'Animal Name',
        'Death/Exit Date':'Death Exit Date',
        'Sex':            'Sex',
        'Line (Short)':   'Line Short',
        'Line (Stock)':   'Line Stock',
        'Genotype':       'Genotype',
        'Birth Date':     'Born Date',
    })

    for col in ['Sample Name', 'Animal Name', 'Preservation']:
        if col not in samples_df.columns:
            raise ValueError(f"Missing required column in samples.csv: '{col}'")
    for col in ['Animal Name', 'Sex', 'Line Short', 'Line Stock', 'Genotype', 'Born Date']:
        if col not in animals_df.columns:
            raise ValueError(f"Missing required column in animals.csv: '{col}'")

    print(f"\nMerging on 'Animal Name'...")
    merged_df = pd.merge(samples_df, animals_df, on='Animal Name', how='inner',
                         suffixes=('_sample', '_animal'))

    unmatched = len(samples_df) - len(merged_df)
    if unmatched > 0:
        print(f"\n⚠ Warning: {unmatched} samples did not match with animal data")
        unmatched_names = samples_df[~samples_df['Animal Name'].isin(animals_df['Animal Name'])]['Animal Name'].unique()
        for name in unmatched_names:
            print(f"  - {name}")

    print(f"\n✓ Matched {len(merged_df)} samples")

    # Sort by animal number, then by sample number within each animal
    def _natural_sort_key(name):
        # Return a zero-padded string so pandas can sort it directly
        parts = re.split(r'(\d+)', str(name))
        return ''.join(p.zfill(10) if p.isdigit() else p.lower() for p in parts)

    def _sample_sort_key(name):
        s = str(name).strip()
        base = s.split('-')[0] if '-' in s else s
        digits = ''.join(filter(str.isdigit, base))
        return int(digits) if digits else 0

    merged_df = merged_df.copy()
    merged_df['_animal_sort'] = merged_df['Animal Name'].apply(_natural_sort_key)
    merged_df['_sample_sort'] = merged_df['Sample Name'].apply(_sample_sort_key)
    merged_df = merged_df.sort_values(['_animal_sort', '_sample_sort']).drop(
        ['_animal_sort', '_sample_sort'], axis=1
    ).reset_index(drop=True)

    return merged_df


def determine_label_type(preservation):
    p = str(preservation).strip().lower()
    if 'oct' in p and 'block' in p:
        return 'skip', 0
    elif 'frozen' in p:
        return 'rna', 1
    elif 'pfa' in p or 'fixed' in p:
        return 'perfusion', 2
    else:
        print(f"  ⚠ Unknown preservation type '{preservation}', defaulting to RNA")
        return 'rna', 1


def safe_date_format(date_value):
    try:
        return pd.to_datetime(date_value).strftime('%m/%d/%y')
    except:
        return str(date_value) if pd.notna(date_value) else 'N/A'


def safe_get(row, *keys, default='N/A'):
    for key in keys:
        if key in row:
            value = row[key]
            if isinstance(value, pd.Series):
                value = value.dropna()
                if not value.empty:
                    return value.iloc[0]
            elif pd.notna(value):
                return value
    return default


clean_genotype_for_label = genotype_to_symbol


def format_sample_number(sample_name, pad=True):
    """
    Format sample name for RNA tube labels.
    Sample names are in the form '1195-0', '1195-C', etc.

    Sides tab (pad=True):  zero-pad base to 4 digits, keep suffix -> '1195-0'
    Tops  tab (pad=False): strip leading zeros from base, keep suffix -> '1195-0'
    """
    try:
        s = str(sample_name).strip()
        if '-' in s:
            num_part = s.rsplit('-', 1)[0]
            suffix   = s.rsplit('-', 1)[1]
        else:
            num_part = s
            suffix   = None
        digits = ''.join(filter(str.isdigit, num_part))
        if not digits:
            return s
        formatted_num = digits.zfill(4) if pad else str(int(digits))
        return f"{formatted_num}-{suffix}" if suffix is not None else formatted_num
    except:
        return str(sample_name)


def format_label_rows(row, label_type):
    """Create the 4 rows of text for each label."""
    harvest_date = safe_date_format(safe_get(row, 'Sample Harvest Date'))
    born_date    = safe_date_format(safe_get(row, 'Born Date'))

    sex_val = safe_get(row, 'Sex')
    sex = str(sex_val).upper()[0] if pd.notna(sex_val) and str(sex_val) != 'N/A' else 'U'

    line_stock_val = safe_get(row, 'Line Stock')
    line_stock = str(line_stock_val).lstrip('0') if pd.notna(line_stock_val) and str(line_stock_val) != 'N/A' else ''

    genotype   = clean_genotype_for_label(safe_get(row, 'Genotype'))
    sample_name = safe_get(row, 'Sample Name')
    animal_name = safe_get(row, 'Animal Name')
    line_short  = safe_get(row, 'Line Short')

    # Calculate age in weeks and days at harvest from birth date + harvest date
    age_weeks = 'N/A'
    age_days_label = 'N/A'
    try:
        bd = pd.to_datetime(safe_get(row, 'Born Date'))
        hd = pd.to_datetime(safe_get(row, 'Sample Harvest Date'))
        if pd.notna(bd) and pd.notna(hd):
            days = (hd - bd).days
            age_weeks = int(days / 7)
            age_days_label = f"P{days}"
    except:
        pass

    row1 = f"{sample_name}_{harvest_date}_{animal_name}"
    row2 = f"{age_weeks}Wks_{sex}_{line_short}_{line_stock}"
    row3 = f"{genotype}_{born_date}_{age_days_label}"
    row4 = "Mouse_Perfused Brain" if label_type.lower() == 'perfusion' else "Mouse_Frozen Brain"

    return row1, row2, row3, row4


def generate_all_labels(merged_df):
    perfusion_labels = []
    rna_labels       = []
    perfusion_count  = 0
    rna_count        = 0
    oct_count        = 0

    print("\nProcessing samples:")
    for _, data_row in merged_df.iterrows():
        preservation = safe_get(data_row, 'Preservation')
        label_type, copies = determine_label_type(preservation)
        sample_name  = safe_get(data_row, 'Sample Name')
        animal_name  = safe_get(data_row, 'Animal Name')

        if label_type == 'skip':
            oct_count += 1
            print(f"  {sample_name}: OCT BLOCK (skipped)")
            continue

        if label_type == 'perfusion':
            perfusion_count += 1
            try:
                row1, row2, row3, row4 = format_label_rows(data_row, label_type)
            except Exception as e:
                print(f"  ✗ Error formatting '{sample_name}': {e}")
                traceback.print_exc()
                continue
            for _ in range(copies):
                perfusion_labels.append({'Row 1': row1, 'Row 2': row2,
                                         'Row 3': row3, 'Row 4': row4})
            print(f"  {sample_name}: PERFUSION ({copies} labels)")

        elif label_type == 'rna':
            rna_count += 1
            try:
                harvest_date   = safe_date_format(safe_get(data_row, 'Sample Harvest Date'))
                line_short     = safe_get(data_row, 'Line Short')
                sample_padded  = format_sample_number(sample_name, pad=True)
                sample_raw     = format_sample_number(sample_name, pad=False)
                animal_str     = str(animal_name).strip()
                rna_labels.append({
                    'Sides_Label_Num': rna_count,
                    'Sides_B':         f"{sample_padded}_{harvest_date}",
                    'Sides_C':         f"{animal_str}_{line_short}",
                    'Tops_Label_Num':  rna_count,
                    'Tops_B':          sample_raw,
                    'Tops_C':          animal_str,
                })
            except Exception as e:
                print(f"  ✗ Error formatting RNA '{sample_name}': {e}")
                traceback.print_exc()
                continue
            print(f"  {sample_name}: RNA (1 label)")

    print(f"\n✓ Summary:")
    print(f"  Perfusion: {perfusion_count} samples × 2 = {perfusion_count * 2} labels")
    print(f"  RNA:       {rna_count} samples × 1 = {rna_count} labels")
    if oct_count:
        print(f"  OCT Block: {oct_count} skipped")

    return perfusion_labels, rna_labels, perfusion_count, rna_count, oct_count


def create_rna_excel(rna_labels, output_folder, timestamp):
    if not rna_labels:
        print("\n⚠ No RNA labels to create.")
        return None

    print("\n" + "=" * 60)
    print("CREATING RNA TUBE LABELER FILE")
    print("=" * 60)

    mismatches = [i + 1 for i, l in enumerate(rna_labels)
                  if l['Sides_Label_Num'] != l['Tops_Label_Num']]
    if mismatches:
        raise ValueError(f"RNA label number mismatch at rows: {mismatches}")
    print(f"  ✓ Label numbers match ({len(rna_labels)} labels)")

    sides_df = pd.DataFrame({
        'Label Number':  [l['Sides_Label_Num'] for l in rna_labels],
        'Sample_Date':   [l['Sides_B']         for l in rna_labels],
        'Animal_Strain': [l['Sides_C']         for l in rna_labels],
    })
    tops_df = pd.DataFrame({
        'Label Number':  [l['Tops_Label_Num'] for l in rna_labels],
        'Sample Number': [l['Tops_B']         for l in rna_labels],
        'Animal Number': [l['Tops_C']         for l in rna_labels],
    })

    output_file = os.path.join(output_folder, f"Tube_Labeler_RNA_{timestamp}.xlsx")
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        sides_df.to_excel(writer, sheet_name='Sides', index=False, header=False)
        tops_df.to_excel(writer,  sheet_name='Tops',  index=False, header=False)

    print(f"  ✓ Saved: {os.path.basename(output_file)}")
    return output_file


def create_single_perfusion_sheet(all_labels, start_index, labels_used,
                                   output_folder, sheet_num, timestamp):
    sheet_labels = [{'Row 1': '', 'Row 2': '', 'Row 3': '', 'Row 4': ''}
                    for _ in range(labels_used)]

    labels_on_sheet = 0
    current_index   = start_index
    while len(sheet_labels) < LABELS_PER_PAGE and current_index < len(all_labels):
        sheet_labels.append(all_labels[current_index])
        current_index  += 1
        labels_on_sheet += 1

    output_file = os.path.join(
        output_folder,
        f"Mailmerge_Labels_PERFUSION_{timestamp}_sheet{sheet_num}.xlsx"
    )
    pd.DataFrame(sheet_labels).to_excel(output_file, index=False, sheet_name='Labels')

    print(f"\n✓ Perfusion sheet {sheet_num}: {os.path.basename(output_file)}")
    print(f"  Empty positions at start: {labels_used}")
    print(f"  Labels on this sheet:     {labels_on_sheet}")
    return labels_on_sheet, output_file


def create_perfusion_mailmerge(perfusion_labels, output_folder, timestamp):
    if not perfusion_labels:
        print("\n⚠ No perfusion labels to create.")
        return 0, []

    print("\n" + "=" * 60)
    print("CREATING PERFUSION MAIL MERGE FILE(S)")
    print("=" * 60)

    created_files       = []
    current_label_index = 0
    sheet_num           = 1

    while current_label_index < len(perfusion_labels):
        remaining = len(perfusion_labels) - current_label_index
        print(f"\n📄 PERFUSION SHEET {sheet_num}  ({remaining} labels remaining)")

        while True:
            try:
                prompt = (f"How many labels already used on this sheet? "
                          f"(0–{LABELS_PER_PAGE-1}, Enter = 0): ")
                val = input(prompt).strip()
                labels_used = int(val) if val else 0
                if 0 <= labels_used < LABELS_PER_PAGE:
                    break
                print(f"Please enter 0–{LABELS_PER_PAGE - 1}")
            except ValueError:
                print("Please enter a valid number")

        available = LABELS_PER_PAGE - labels_used
        to_place  = min(available, remaining)
        print(f"  → Placing {to_place} labels on this sheet")

        placed, output_file = create_single_perfusion_sheet(
            perfusion_labels, current_label_index, labels_used,
            output_folder, sheet_num, timestamp
        )
        created_files.append(output_file)
        current_label_index += placed
        sheet_num += 1

        if current_label_index < len(perfusion_labels):
            print(f"\n⚠ {len(perfusion_labels) - current_label_index} labels still to place.")
            input("Press Enter when ready for the next sheet...")

    return len(created_files), created_files


def main():
    script_dir   = os.path.dirname(os.path.abspath(__file__))
    samples_file = os.path.join(script_dir, INPUT_SAMPLES_FILE)
    animals_file = os.path.join(script_dir, INPUT_ANIMALS_FILE)

    try:
        print("=" * 60)
        print("LABEL GENERATOR")
        print("=" * 60)
        print(f"Working directory: {script_dir}\n")

        for path, name in [(samples_file, INPUT_SAMPLES_FILE),
                           (animals_file, INPUT_ANIMALS_FILE)]:
            if not os.path.exists(path):
                print(f"\n✗ ERROR: File not found: {name}")
                return False

        merged_df = load_and_merge_data(samples_file, animals_file)
        if len(merged_df) == 0:
            print("\n✗ No matching records found between samples and animals.")
            return False

        perfusion_labels, rna_labels, perf_count, rna_count, oct_count = \
            generate_all_labels(merged_df)

        if not perfusion_labels and not rna_labels:
            print("\n✗ No labels created.")
            return False

        timestamp     = datetime.now().strftime('%Y%m%d_%H%M%S')
        created_files = []

        if rna_labels:
            f = create_rna_excel(rna_labels, script_dir, timestamp)
            if f:
                created_files.append(f)

        if perfusion_labels:
            _, perf_files = create_perfusion_mailmerge(
                perfusion_labels, script_dir, timestamp)
            created_files.extend(perf_files)

        print(f"\n{'=' * 60}")
        print(f"✓ SUCCESS — {len(created_files)} file(s) created:")
        for f in created_files:
            print(f"  📄 {os.path.basename(f)}")

        if oct_count:
            print(f"\nNote: {oct_count} OCT Block sample(s) skipped (no labels needed)")

        if perfusion_labels:
            print("\nPerfusion Mail Merge next steps:")
            print("  1. Open the perfusion Excel file in Word Mail Merge")
            print("  2. Use fields: «Row_1», «Row_2», «Row_3», «Row_4»")
            print("  3. Empty rows pad labels to the correct starting position")

        return True

    except KeyboardInterrupt:
        print("\n\n✗ Cancelled by user")
        return False
    except Exception as e:
        print(f"\n✗ ERROR: {type(e).__name__}: {e}")
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    print()
    input("Press Enter to exit...")
    sys.exit(0 if success else 1)
