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
df_case1 = pd.read_csv('preprocessed_2.csv', compression = 'gzip')
df_case1 = df_case1[~((df_case1.duplicated(subset=['number','pick_lat','pick_lng'],keep=False)) & (df_case1.booking_time_diff_hr<=1))]

# Preprocess Case 1
df_case1.to_csv('Case1.csv',index = False, compression = 'gzip')
### Case 2
df_case2 = pd.read_csv('Case1.csv', compression = 'gzip')
print("Number of rides booked by same customer within 8mins time: {}".format(len(df_case2[(df_case2.booking_time_diff_min<8)])))
df_case2 = df_case2[(df_case2.booking_time_diff_min>=8)]

# Using geodesic distance (shortest distance between a long/lat)
# Function to find geodestic_distance
def geodestic_distance(pick_lat, pick_lng, drop_lat, drop_lng):
    # 1 mile = 1.60934 km
    distance = round(geodesic((pick_lat, pick_lng), (drop_lat,drop_lng)).miles*1.60934, 2)
    return distance
# Commented this out for now because it takes forever to run
df_case2['geodesic_distance'] = np.vectorize(geodestic_distance)(df_case2['pick_lat'],df_case2['pick_lng'],df_case2['drop_lat'],df_case2['pick_lng'])

# Preprocess Case 2
df_case2.to_csv('Case2.csv',index = False, compression = 'gzip')

# Case 2.1 Removing ride request less than 0.05 miles = 50 meters
df_case2_1 = pd.read_csv('Case2.csv', compression = 'gzip')
print("Number of Rides Requests less than 50meters: {}".format(len(df_case2_1[df_case2_1.geodesic_distance<=0.05])))

# Remove ride requests
df_case2_1 = df_case2_1[df_case2_1.geodesic_distance>0.05]
df_case2_1.to_csv('Case2_1.csv',index = False, compression = 'gzip')

### Case 3
# India: 'boundingbox': ['6.2325274', '35.6745457', '68.1113787', '97.395561']
# Bangalore: 'boundingbox': ['12.8340125', '13.1436649', '77.4601025', '77.7840515']
# Karnataka: 'boundingbox': ['11.5945587', '18.4767308', '74.0543908', '78.588083']

df_case3 = pd.read_csv('Case2_1.csv', compression = 'gzip')

# Returns raw location data
location = geolocator.geocode("India")

# Rides outside of india
df_case3.reset_index(inplace = True, drop = True)
outside_India = df_case3[(df_case3.pick_lat<=6.2325274) | (df_case3.pick_lat>=35.6745457) | (df_case3.pick_lng<=68.1113787) | (df_case3.pick_lng>=97.395561) | (df_case3.drop_lat<=6.2325274) | (df_case3.drop_lat>=35.6745457) | (df_case3.drop_lng<=68.1113787) | (df_case3.drop_lng>=97.395561)]

# Reset index while also dropping anything that is in outside india
df_case3 = df_case3[~df_case3.index.isin(outside_India.index)].reset_index(drop = True)

# Number of good ride requests
print("Number of Good Ride Requests: {}".format(len(df_case3)))

# Pickups outside Bangalore
pck_outside_bng = df_case3[(df_case3.pick_lat<=12.8340125) | (df_case3.pick_lat>=13.1436649) | (df_case3.pick_lng<=77.4601025) | (df_case3.pick_lng>=77.7840515)]
# Dropoffs outside Bangalore
drp_outside_bng = df_case3[(df_case3.drop_lat<=12.8340125) | (df_case3.drop_lat>=13.1436649) | (df_case3.drop_lng<=77.4601025) | (df_case3.drop_lng>=77.7840515)]

# Bouding pickup within state karnataka
pck_outside_KA = df_case3[(df_case3.pick_lat<=11.5945587) | (df_case3.pick_lat>=18.4767308) | (df_case3.pick_lng<=74.0543908) | (df_case3.pick_lng>=78.588083)]
drp_outside_KA = df_case3[(df_case3.drop_lat<=11.5945587) | (df_case3.drop_lat>=18.4767308) | (df_case3.drop_lng<=74.0543908) | (df_case3.drop_lng>=78.588083)]

# Total rides outside karnataka:
total_ride_outside_KA = df_case3[(df_case3.pick_lat<=11.5945587) | (df_case3.pick_lat>=18.4767308) | (df_case3.pick_lng<=74.0543908) | (df_case3.pick_lng>=78.588083) | (df_case3.drop_lat<=11.5945587) | (df_case3.drop_lat>=18.4767308) | (df_case3.drop_lng<=74.0543908) | (df_case3.drop_lng>=78.588083)]

print("Total Ride Outside Karnataka: {}".format(len(total_ride_outside_KA)))

# Suspected bad rides
suspected_bad_rides = total_ride_outside_KA[total_ride_outside_KA.geodesic_distance > 500]

# Remove suspected bad rides
df_case3 = df_case3[~df_case3.index.isin(suspected_bad_rides.index)].reset_index(drop = True)

# Final cleaned dataset
dataset = df_case3[['ts', 'number', 'pick_lat','pick_lng','drop_lat','drop_lng','geodesic_distance','hour','mins','day','month','year','dayofweek','booking_timestamp','booking_time_diff_hr', 'booking_time_diff_min']]

# Final cleaned dataset to csv
dataset.to_csv('clean_data.csv',index = False, compression = 'gzip')

# Final print
df = pd.read_csv('clean_data.csv', compression = 'gzip')
print("Number of Good Ride Requests: {}".format(len(df)))




