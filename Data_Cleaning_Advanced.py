## Clean our imported raw data but also following more of the set guidelines

# Imports
import pandas as pd
import numpy as np
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
import time

# Geolocator
geolocator = Nominatim(user_agent="OLABikes")

# Read data from our basic cleaning
df = pd.read_csv('preprocessed_1.csv', compression = 'gzip')

### Data cleaning with our business understanding

## Case 1 "Rebooking again to the same location" - keep only one request in 1 hour time frame of first request

## Case 2 "Location entry mistake" - keep only last request of user within 8 minutes of first booking request

## Case 3 "Booking location outside operation zone"


## Checking lat-long bounding box coordinates
# Sorting out our general information
df['ts'] = pd.to_datetime(df['ts'])
df.sort_values(by = ['number','ts'], inplace=True)
df.reset_index(inplace=True)

# Convert to numpy array and cast to int64 - output is in ns so divide by 10^9
df['booking_timestamp'] = df.ts.values.astype(np.int64)// 10 ** 9

# Create a new column that shifts booking timestamp down by 1
df['shift_booking_ts'] = df.groupby('number')['booking_timestamp'].shift(1)
# Because we shifted a row there will be a missing value in the first index
df['shift_booking_ts'].fillna(0, inplace = True)

# Convert our shifted row to int64
df['shift_booking_ts'] = df['shift_booking_ts'].astype('int64')

# Create a new column that figures our the hour difference between our booking and shifted booking
df['booking_time_diff_hr'] = round((df['booking_timestamp'] - df['shift_booking_ts'])//3600)
# Create a new column that figures our the minute difference between our booking and shifted booking
df['booking_time_diff_min'] = round((df['booking_timestamp'] - df['shift_booking_ts'])//60)

# Create a dictionary containing our difference in minutes for each index
df['booking_time_diff_min'].value_counts().to_dict()
# Create a dictionary containing our difference in hours for each index
df['booking_time_diff_hr'].value_counts().to_dict()

#Preprocess
df.to_csv('preprocessed_2.csv',index = False, compression = 'gzip')
### Case 1
# Make our dataframe all the values that are not duplicated number and location within an hour
df = df[~((df.duplicated(subset=['number','pick_lat','pick_lng'],keep=False)) & (df.booking_time_diff_hr<=1))]

# Preprocess Case 1
df.to_csv('Case1.csv',index = False, compression = 'gzip')
### Case 2
print("Number of rides booked by same customer within 8mins time: {}".format(len(df[(df.booking_time_diff_min<8)])))
df = df[(df.booking_time_diff_min>=8)]

# Using geodesic distance (shortest distance between a long/lat)
# Function to find geodestic_distance
def geodestic_distance(pick_lat, pick_lng, drop_lat, drop_lng):
    # 1 mile = 1.60934 km
    distance = round(geodesic((pick_lat, pick_lng), (drop_lat,drop_lng)).miles*1.60934, 2)
    return distance
# Commented this out for now because it takes forever to run
#df['geodesic_distance'] = np.vectorize(geodestic_distance)(df['pick_lat'],df['pick_lng'],df['drop_lat'],df['pick_lng'])

# Preprocess Case 2
#df.to_csv('Case2.csv',index = False, compression = 'gzip')

df = pd.read_csv('Case2.csv', compression = 'gzip')
print("Number of Rides Requests less than 50meters: {}".format(len(df[df.geodesic_distance<=0.05])))

