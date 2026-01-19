import pandas as pd
import sys

def compare_csv_by_property_id(file1, file2):
    # Load the CSVs
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)

    key_col = "Property ID"
    def_col = "IsDefaulter"

    # Set index to Property ID for easy lookup
    df1.set_index(key_col, inplace=True)
    df2.set_index(key_col, inplace=True)

    # Find common and unique Property IDs
    common_ids = df1.index.intersection(df2.index)
    unique_in_df1 = df1.index.difference(df2.index)
    unique_in_df2 = df2.index.difference(df1.index)

    print(f"Common Property IDs: {len(common_ids)}")
    print(f"Unique Property IDs in File 1: {len(unique_in_df1)}")
    print(f"Unique Property IDs in File 2: {len(unique_in_df2)}")

    # 1️⃣ Compare rows in common Property IDs
    print("\n=== Checking row-by-row differences for common Property IDs ===")
    differences = 0
    for prop_id in common_ids:
        row1 = df1.loc[prop_id]
        row2 = df2.loc[prop_id]

        if not row1.equals(row2):
            differences += 1
            diffs = row1 != row2
            differing_cols = row1.index[diffs].tolist()
            print(f"\nProperty ID {prop_id} differs in columns: {differing_cols}")
            print(f"  File1: {row1[differing_cols].to_dict()}")
            print(f"  File2: {row2[differing_cols].to_dict()}")

    if differences == 0:
        print("\nAll common Property IDs have identical rows.")
    else:
        print(f"\nTotal differing Property IDs: {differences}")

    # 2️⃣ Defaulter check in the larger file
    print("\n=== Checking Defaulter consistency ===")
    if len(df1) >= len(df2):
        larger_df, smaller_df = df1, df2
        larger_file, smaller_file = file1, file2
    else:
        larger_df, smaller_df = df2, df1
        larger_file, smaller_file = file2, file1

    inconsistent_defaulters = []
    for prop_id in larger_df.index:
        def_status = larger_df.at[prop_id, def_col]
        if def_status == "Yes":
            if prop_id in smaller_df.index:
                other_def_status = smaller_df.at[prop_id, def_col]
                if other_def_status != "Yes":
                    inconsistent_defaulters.append(prop_id)
            else:
                # The defaulter is missing from the smaller file
                inconsistent_defaulters.append(prop_id)

    if inconsistent_defaulters:
        print(f"\nDefaulter inconsistencies found in {len(inconsistent_defaulters)} Property IDs:")
        for prop_id in inconsistent_defaulters:
            print(f" - {prop_id}")
    else:
        print("\nAll defaulters are consistent across files.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python compare_by_property_id.py file1.csv file2.csv")
        sys.exit(1)

    file1 = sys.argv[1]
    file2 = sys.argv[2]

    compare_csv_by_property_id(file1, file2)
