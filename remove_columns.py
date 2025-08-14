import os
import pandas as pd

# folder containing CSV files
folder_path = '/home/prerna/Punjab/punjab-data-prod-analysis/kharar/ouput_demand_detail/'

# columns to remove
columns_to_remove = [
    'createdby', 'createdtime', 'lastmodifiedby', 
    'lastmodifiedtime', 'tenantid', 'additionaldetails'
]

for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        file_path = os.path.join(folder_path, filename)
        print(f'Processing file: {file_path}')
        
        # read CSV
        df = pd.read_csv(file_path)
        
        # drop columns if they exist
        df = df.drop(columns=[col for col in columns_to_remove if col in df.columns])
        
        # overwrite original file (or change to write to new file)
        df.to_csv(file_path, index=False)
        print(f'Finished: {file_path}')

print('All files processed.')
