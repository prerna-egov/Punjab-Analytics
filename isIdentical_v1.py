import pandas as pd
import sys
import ast
import numpy as np

def load_and_prepare_df(filepath):
    # Load CSV and force Property ID to string, strip whitespace
    df = pd.read_csv(filepath, dtype=str, low_memory=False)
    df['Property ID'] = df['Property ID'].str.strip()

    # Check for duplicates
    duplicates = df['Property ID'].duplicated().sum()
    if duplicates > 0:
        print(f"WARNING: {duplicates} duplicate Property IDs found in {filepath}")

    # Set index for lookup
    df.set_index('Property ID', inplace=True)
    return df


def rows_are_equivalent(row1, row2, float_tolerance=1e-6):
    for col in row1.index:
        val1 = row1[col]
        val2 = row2[col]

        # Try to parse list-like string
        try:
            parsed1 = ast.literal_eval(val1) if isinstance(val1, str) and val1.startswith("[") else val1
            parsed2 = ast.literal_eval(val2) if isinstance(val2, str) and val2.startswith("[") else val2

            # If both are lists, compare as sorted lists
            if isinstance(parsed1, list) and isinstance(parsed2, list):
                if sorted(parsed1) != sorted(parsed2):
                    return False
                continue  # Match found
        except (ValueError, SyntaxError):
            parsed1 = val1
            parsed2 = val2

        # Numeric comparison (handle int, float, numeric strings)
        try:
            num1 = float(parsed1)
            num2 = float(parsed2)
            if not np.isclose(num1, num2, atol=float_tolerance):
                return False
            continue
        except (ValueError, TypeError):
            pass  # Not numeric, fallback to string

        # Final string comparison after stripping whitespace
        if str(parsed1).strip() != str(parsed2).strip():
            return False

    return True


def compare_csv_by_property_id(file1, file2, output_mismatch_file='row_mismatches.csv'):
    # Load and preprocess data
    df1 = load_and_prepare_df(file1)
    df2 = load_and_prepare_df(file2)

    # Debug: sample Property IDs
    print(f"Sample Property IDs in file1: {list(df1.index)[:5]}")
    print(f"Sample Property IDs in file2: {list(df2.index)[:5]}")

    # Find common and unique Property IDs
    common_ids = df1.index.intersection(df2.index)
    unique_in_df1 = df1.index.difference(df2.index)
    unique_in_df2 = df2.index.difference(df1.index)

    print(f"\nCommon Property IDs: {len(common_ids)}")
    print(f"Unique Property IDs in File 1: {len(unique_in_df1)}")
    print(f"Unique Property IDs in File 2: {len(unique_in_df2)}")

    # Collect mismatches
    mismatches = []
    for prop_id in common_ids:
        row1 = df1.loc[prop_id]
        row2 = df2.loc[prop_id]

        if not rows_are_equivalent(row1, row2):
            diffs = [
                col for col in row1.index
                if not rows_are_equivalent(pd.Series({col: row1[col]}), pd.Series({col: row2[col]}))
            ]
            
            mismatch_entry = {
                'Property ID': prop_id,
                'Differing Columns': ', '.join(diffs),
                'File1 Values': row1[diffs].to_dict(),
                'File2 Values': row2[diffs].to_dict()
            }
            mismatches.append(mismatch_entry)

    # Export mismatches to CSV
    if mismatches:
        mismatch_df = pd.DataFrame(mismatches)
        mismatch_df.to_csv(output_mismatch_file, index=False)
        print(f"\nMismatch report saved to '{output_mismatch_file}' with {len(mismatches)} mismatched rows.")
    else:
        print("\nAll common Property IDs have identical rows.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_by_property_id.py file1.csv file2.csv")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    compare_csv_by_property_id(file1, file2)
