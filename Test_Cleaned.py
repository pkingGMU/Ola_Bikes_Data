import pandas as pd

df = pd.read_csv('clean_data.csv', compression = 'gzip')
print("Number of Good Ride Requests: {}".format(len(df)))