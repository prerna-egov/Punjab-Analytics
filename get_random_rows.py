import csv
import random

# Input and output file paths
input_file = '/home/prerna/Downloads/Punjab_Data_Analysis_Phagwara_PDC_FINAL_amountDue_1.csv'
output_file = '15_random_defaulters_2.csv'

# Read the CSV file
with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
    reader = list(csv.reader(csvfile))
    header = reader[0]          # Assume the first row is header
    data = reader[1:]           # All rows except the header

# Pick 15 random rows
random_rows = random.sample(data, 15)

# Write the selected rows to a new CSV
with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(header)    # Write header first
    writer.writerows(random_rows)

print(f"15 random rows written to {output_file}")
