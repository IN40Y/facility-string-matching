import pandas as pd
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
import re


df1 = pd.read_csv('unvalidated.csv')
df2 = pd.read_csv('correct_list.csv')


#drop the index
df2 = df2.drop(df2.columns[0], axis=1)

#preprocessing function to remove special characters and convert to lowercase
def preprocess_text(text):
    if pd.notna(text):  # Check if the value is not NaN
        # Remove special characters using regex
        cleaned_text = re.sub(r'[^A-Za-z0-9\s]', '', text)
        # Convert to lowercase (optional)
        cleaned_text = cleaned_text.lower()
        return cleaned_text
    else:
        return text

# Apply the preprocessing function to the column
df1['lga'] = df1['lga'].apply(preprocess_text)

def similarity_score(str1, str2):
    return fuzz.token_set_ratio(str1.lower(), str2.lower())

# Ensure that the columns you're comparing are of string data type
df1['facility_name'] = df1['facility_name'].astype(str)
df1['lga'] = df1['lga'].astype(str)
df2['facility_name'] = df2['facility_name'].astype(str)
df2['lga'] = df2['lga'].astype(str)

# Function to calculate similarity score
def similarity_score(a, b):
    return fuzz.token_set_ratio(a, b)

# Function to clean and preprocess strings
def clean_and_preprocess(s):
    # Convert to string, strip leading/trailing whitespace, and replace empty strings with a placeholder
    return str(s).strip() if str(s).strip() != '' else 'N/A'

# Apply the cleaning and preprocessing function to the 'lga' column
df1['lga'] = df1['lga'].apply(clean_and_preprocess)
df2['lga'] = df2['lga'].apply(clean_and_preprocess)

# Iterate through all rows in df1
for idx1 in range(len(df1)):
    # Compare Facility Name
    facility_matches = process.extractOne(df1.at[idx1, 'facility_name'], df2['facility_name'], scorer=fuzz.token_set_ratio)

    # Compare LGA
    lga_matches = process.extractOne(df1.at[idx1, 'lga'], df2['lga'], scorer=fuzz.token_set_ratio)

    if facility_matches[1] > 70 and lga_matches[1] > 70:  # Adjust the similarity threshold as needed
        matched_facility_idx = facility_matches[2]
        matched_lga_idx = lga_matches[2]
            
        matched_facility_row = df2.iloc[matched_facility_idx]
        matched_lga_row = df2.iloc[matched_lga_idx]
            
        df1.at[idx1, 'Validated facility name'] = matched_facility_row['facility_name']
        df1.at[idx1, 'LGA.1'] = matched_lga_row['lga']
        df1.at[idx1, 'facility_level'] = matched_facility_row['facility_level']
        df1.at[idx1, 'longitude'] = matched_facility_row['longitude']
        df1.at[idx1, 'latitude'] = matched_facility_row['latitude']
        df1.at[idx1, 'physical_location'] = matched_facility_row['physical_location']
        df1.at[idx1, 'postal_address'] = matched_facility_row['postal_address']
        df1.at[idx1, 'hiv_capable'] = matched_facility_row['hiv_capable']

df1.to_csv('corrected_dataset.csv', index=False) 