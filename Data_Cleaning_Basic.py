## Clean our imported raw data

# Imports
import pandas as pd
import numpy as np

# Set our dataframe equal to the raw data csv
# We use gzip to compress our data
df = pd.read_csv('raw_data.csv', low_memory=False, compression='gzip')

# Viewing our dataframe in the terminal
print(df.head(3))

# We can now see there is a ts(timestamp), id, pick/drop lat/lng
# The instructions told us there can only be one number per timestamp
# Remove duplicates but keep the first occurence
df.drop_duplicates(subset=['ts','number'], inplace = True, keep='last')

# Reset our index after removing duplicates
df.reset_index(inplace = True, drop= True)

# Convert all 'number' values to numbers
df['number'] = pd.to_numeric(df['number'], errors='coerce')
# Count missing values (is null) from dataset
print(f' Missing values after conversion: {np.count_nonzero(df.isnull().values)}')

# Drop any rows that return NaN for number
# We use inplace since we are actively altering the data
df.dropna(inplace=True)

# After dropping our missing values perform the conversion again
df['number'] = pd.to_numeric(df['number'], errors = 'coerce', downcast='integer')
# Convert our timestamp to datetime 
df['ts'] = pd.to_datetime(df['ts'])

## Breaking down a timestamp
df['hour'] = df['ts'].dt.hour
df['mins'] = df['ts'].dt.minute
df['day'] = df['ts'].dt.day
df['month'] = df['ts'].dt.month
df['year'] = df['ts'].dt.year
df['dayofweek'] = df['ts'].dt.dayofweek

# Export to csv
df.to_csv('preprocessed_1.csv',index = False, compression = 'gzip')





