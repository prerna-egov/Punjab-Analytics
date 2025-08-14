import os
import pandas as pd


folder_path = '/home/prerna/Punjab/punjab-data-prod-analysis/kharar/ouput_demand_detail/'

# property_df = pd.read_csv('property.csv')
# unit_df = pd.read_csv('property_unit.csv')
demand_df = pd.read_csv('/home/prerna/Punjab/punjab-data-prod-analysis/kharar/egbs_demand_v1.csv')
# demand_details_df = pd.read_csv('demand_details.csv')

# collect DataFrames into a list
all_chunks = []

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        print(f'Loading file: {file_path}')
        df = pd.read_csv(file_path)
        all_chunks.append(df)

# concatenate all chunks into a single DataFrame
merged_df = pd.concat(all_chunks, ignore_index=True)


# join
matching_demand_details = merged_df[merged_df['demandid'].isin(demand_df['id'])]
count_matching = len(matching_demand_details)


print(f'Number of demandDetails rows with matching demandIds: {count_matching}')