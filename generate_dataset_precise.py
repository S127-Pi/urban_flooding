#Code from Thijs Simons is used: https://github.com/SimonsThijs/wateroverlast
# import rdconverter
import rdconverter
from neerslag import precipitation_nl
from layer import ahn_layer
from datetime import datetime
import pandas as pd
import random
import time
import os 
from BAG import home_sampler

# Date time variables
begin = '2016-01-02 00:00:00'
end = '2022-01-30 23:59:59'
strfformat = "%Y-%m-%d %H:%M:%S"
strfformat_ensurance = "%d/%m/%Y"

dire = os.path.dirname(__file__)
input_file = "/precise_w_transformed.json"
output_file = "precise_1_ouput_without_postcode.pkl"

# Time variables
total = 0
count = 0
btime = time.time()

# Rain threshold
rain_treshold = 500


def parse_datetime(data, ensurance=False):
    if ensurance:
        return datetime.strptime(str(data['date']).strip(), strfformat_ensurance)
    return datetime.strptime(str(data['date']).strip(), strfformat)


def get_precipitation_data_ensurance(row):
    date = row['date']

    global count
    count += 1
    now = time.time()
    avg_time = (now-btime)/count
    left = total-count
    if count % 10 == 0:
        print('====== rain ')
        print('time spent', now-btime)
        print('did', count, 'examples')
        print('avg', avg_time)
        print('left', left)
        print('time left', left*avg_time)
        print('======')
    lat = row['lat']
    lon = row['lng']
    rain = PNL.get_precipation_data_past_hours_list(date.year, date.month, date.day, 23, 59, lat, lon, 24)
    peak = 0
    for idx, sum in enumerate(rain):
        sum_3hours = sum + rain[idx + 1] + rain[idx + 2]
        if sum_3hours > peak:
            peak = sum_3hours
    return peak


def get_precipitation_data(row):
    global count
    count += 1
    now = time.time()
    avg_time = (now-btime)/count
    left = total-count
    if count % 10 == 0:
        print('====== rain ')
        print('time spent', now-btime)
        print('did', count, 'examples')
        print('avg', avg_time)
        print('left', left)
        print('time left', left*avg_time)
        print('======')

    date = row['date']
    date = datetime.strptime(str(row['date']), strfformat)
    lat = row['lat']
    lon = row['lng']

    return get_past3hours(date, lat, lon)


def get_past3hours(date, lat, lon):
    rain = PNL.get_precipation_data_past_hours_list(date.year, date.month, date.day, date.hour, date.minute, lat, lon, 3)
    return sum(rain)


# Step 1: Loading file and assigning target value
df1 = pd.read_json(dire + input_file)
df1 = df1.dropna()
df1['target'] = 1
df1 = df1.drop("zipcode", axis=1)

# Step 2: Filter out dates which are out of the scope
df1['date'] = df1.apply(parse_datetime, axis=1)
df1 = df1[(begin < df1['date']) & (df1['date'] < end)]

# Step 3: Enrich instances with rain information
PNL = precipitation_nl.PrecipitationNL(queue_size=300)
total = len(df1)
count = 0
btime = time.time()
df1 = df1.sort_values(by=['date'])
df1['past3hours'] = df1.apply(get_precipitation_data, axis=1)

# Step 4: Filter out water damage notifications which are not caused by rain
df1 = df1[(df1.past3hours > rain_treshold)]

# #Step 5: Add height layer as a feature
total = len(df1)
count = 0
btime = time.time()
ahn = ahn_layer.AHNLayer()


def add_layers(data):
    try:
        global count
        count += 1
        now = time.time()
        avg_time = (now-btime)/count
        left = total-count
        if count % 10 == 0:
            print('====== layers ')
            print('time spent', now-btime)
            print('did', count, 'examples')
            print('avg', avg_time)
            print('left', left)
            print('time left', left*avg_time)
            print('======')

        lat = data['lat']
        lng = data['lng']
        rdx = rdconverter.gps2X(lat, lng)
        rdy = rdconverter.gps2Y(lat, lng)

        x, y = round(rdx, 2), round(rdy, 2)
        d = 5
        arr = ahn.get_gdal_dataset(x-d, x+d, y-d, y+d)
        arr = arr.ReadAsArray()
        return arr
    except Exception as e:
        print(e)
        return None


df1['layers'] = df1.apply(add_layers, axis=1)
# df1 = df1.drop(columns=['lat', 'lng'])

# Step 6: Add negative examples
# data0 = {'target': [], 'past3hours': [], 'layers': [], "date": [], 'bouwjaar':[]}
data0 = {'target': [],'lat':[], 'lng':[], 'date':[]}

HM = home_sampler.HomeSampler()
n = 1
def sample_random_houses_close(data):
    rdx = rdconverter.gps2X(data['lat'],data['lng'])
    rdy = rdconverter.gps2Y(data['lat'],data['lng'])
    da = HM.sample_in_range(rdx, rdy, 500, 50, n)
    for d in da:
        lat = rdconverter.RD2lat(d[0], d[1])
        lng = rdconverter.RD2lng(d[0], d[1])
        data0['lat'].append(lat)
        data0['lng'].append(lng)
        data0['date'].append(data['date'])
        data0['target'].append(0)

#Adding negatives examples
df1.apply(sample_random_houses_close, axis=1)
df0 = pd.DataFrame(data0)

total = len(df0)
count = 0
btime = time.time()
df0 = df0.sort_values(by=['date'])
df0['past3hours'] = df0.apply(get_precipitation_data, axis=1)

total = len(df0)
count = 0
btime = time.time()
df0['layers'] = df0.apply(add_layers, axis=1)


df = pd.concat([df0, df1], ignore_index=True)
df = df.sort_values(by=['date'])
# ahn = ahn_layer.AHNLayer()
# total = len(df)
# count = 0
# btime = time.time()
# df['layers'] = df.apply(add_layers, axis=1)

# Step 7: Output dataframe as pkl to keep the size of the file small
print(df)
df.to_pickle(f"{dire}/pkl/{output_file}", protocol=4)
