# import pandas as pd
# import numpy as np

# # Load the two CSV files
# file1 = '/home/prerna/Downloads/Punjab_Data_Analysis_Phagwara_PDC_FINAL_amountDue_1.csv'  # Replace with your first file path
# file2 = 'Phagwara_defaulter_report_with_due_amount.csv'  # Replace with your second file path

# df1 = pd.read_csv(file1)
# df2 = pd.read_csv(file2)

# # Ensure 'Property ID' is treated consistently (as string, to avoid type mismatches)
# df1['Property ID'] = df1['Property ID'].astype(str)
# df2['Property ID'] = df2['Property ID'].astype(str)

# # Find common Property IDs
# common_ids = set(df1['Property ID']).intersection(set(df2['Property ID']))

# # Filter DataFrames to only include common Property IDs
# df1_common = df1[df1['Property ID'].isin(common_ids)].set_index('Property ID')
# df2_common = df2[df2['Property ID'].isin(common_ids)].set_index('Property ID')

# # List to collect differences
# differences = []

# # Iterate over common Property IDs and compare row values
# for prop_id in common_ids:
#     row1 = df1_common.loc[prop_id]
#     row2 = df2_common.loc[prop_id]

#     for col in df1.columns:
#         if col == 'Property ID':
#             continue

#         val1 = row1[col]
#         val2 = row2[col]

#         # Handle type differences by converting to float when possible
#         try:
#             val1_num = float(val1)
#             val2_num = float(val2)
#             if not np.isclose(val1_num, val2_num, equal_nan=True):
#                 differences.append({
#                     'Property ID': prop_id,
#                     'Column': col,
#                     'Value in File 1': val1,
#                     'Value in File 2': val2
#                 })
#         except (ValueError, TypeError):
#             # Fallback to string comparison for non-numeric columns
#             if str(val1) != str(val2):
#                 differences.append({
#                     'Property ID': prop_id,
#                     'Column': col,
#                     'Value in File 1': val1,
#                     'Value in File 2': val2
#                 })

# # Create a DataFrame of differences
# diff_df = pd.DataFrame(differences)

# # Save the differences to a CSV
# diff_df.to_csv('differences.csv', index=False)

# print("Comparison complete. Differences saved to 'differences.csv'")




import pandas as pd

# Load the two CSV files
file1 = '/home/prerna/Downloads/Punjab_Data_Analysis_Phagwara_PDC_FINAL_amountDue_1.csv'  # Replace with your first file path
file2 = 'Phagwara_defaulter_report_with_due_amount.csv'  # Replace with your second file path

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Ensure 'Property ID' is treated consistently (as string)
df1['Property ID'] = df1['Property ID'].astype(str)
df2['Property ID'] = df2['Property ID'].astype(str)

# Normalize IsDefaulter column (lowercase and strip spaces)
df1['IsDefaulter'] = df1['IsDefaulter'].str.strip().str.lower()
df2['IsDefaulter'] = df2['IsDefaulter'].str.strip().str.lower()

# Merge the two DataFrames on Property ID
merged_df = pd.merge(df1, df2, on='Property ID', suffixes=('_file1', '_file2'))

# Filter where IsDefaulter status differs
mismatch_df = merged_df[merged_df['IsDefaulter_file1'] != merged_df['IsDefaulter_file2']]

# Save the result
mismatch_df.to_csv('defaulter_mismatches.csv', index=False)

print(f"Found {len(mismatch_df)} mismatches. Saved to 'defaulter_mismatches.csv'")

