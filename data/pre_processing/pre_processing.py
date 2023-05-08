import pandas as pd
import os
from collections import defaultdict
from additional_features.seasons_feature import add_seasons
from additional_features.latitude_longitude_feature import add_lat_lon

# Function fills missing data and saves final dataset in "fian_data"
def fill_missing_data(df:pd.DataFrame):
    # Apply forward and backward fill
    df_ffill_bfill = df.copy()
    df_ffill_bfill = df_ffill_bfill.ffill(axis=0)
    df_ffill_bfill = df_ffill_bfill.bfill(axis=0)
    df_ffill_bfill = df_ffill_bfill.fillna(0.0)
    return df_ffill_bfill


# Functions determins if dataset is included in final trining set
def keep_data_set(df:pd.DataFrame, MAX_NAN_PERCENTAGE:float, filename, colums_over_max_nan) -> bool:
    keepDataSet = True
    n, m = df.shape 

    # Count number of nan values in each column
    for (columnName, columnData) in df.items():
        count_nan = columnData.isnull().sum()
        percent_nan = count_nan/n

        # RF is an important meterological feature. So we included it no matter
        # the % of nan values.
        if percent_nan > MAX_NAN_PERCENTAGE and columnName != 'RF(mm)':
            # Keep track of colums with more than MAX_NAN_PERCENTAGE nans
            colums_over_max_nan[columnName] += 1
            print("We drop", filename.name + "!", columnName, "has more than", MAX_NAN_PERCENTAGE, "nan.")
            keepDataSet = False
    return keepDataSet


def drop_columns(df):
    # We drop features features that we won't use in out final dataset. The features we drop are based
    # on experiments we ran, such as feature correlation. 

    # __________________________________________________________________________
    # The following is a list of all column names and there respectivie index
    # 0           
    # Station Name
    # 1     2   3   4   5     6      7     8         9          10     
    # From	FT	To	TT  PM10  PM2.5	 AT()  BP(mmHg)  SR(W/mt2)  RH(%)	 
    # 11          12      13         14        15          16          17           
    # WD(degree)  RF(mm)  NO(ug/m3)	 NOx(ppb)  NO2(ug/m3)  NH3(ug/m3)  SO2(ug/m3)		
    # 18         19            20         21         22        23           24          
    # CO(mg/m3)	 Ozone(ug/m3)  Benzene()  Toluene()  Xylene()  MP-Xylene()  Eth-Xylene()
    # __________________________________________________________________________

    # We drop the following features and provide a short explination
    drop_columns = [# columns[0], # Station Name (We don't need this feature)
                    # columns[1], # From (We don't need this feature)
                    # columns[2], # FT (WE SHOULD CONSIDER USING THIS FEATURE)
                    # columns[3], # To (We don't need this feature)
                    # columns[4], # TT (WE SHOULD CONSIDER USING THIS FEATURE)
                    # columns[7], # AT (11 'AT' columns have more than MAX_NAN_PERCENTAGE %)
                    # columns[8], # BP (11 'BP' columns have more than MAX_NAN_PERCENTAGE %)
                    # columns[9], # SR (More than 90% empty on some files, we drop this feature instead of entire datasets)
                    # columns[10],# RH (More than 90% empty on some files, we drop this feature instead of entire datasets)
                    # columns[11],# WD (More than 90% empty on some files, we drop this feature instead of entire datasets)
                    # columns[12],# RF (12 'RF' columns have more than MAX_NAN_PERCENTAGE %)
                    df.columns[13],# NO ('NO' and 'NOx' are highly correlated, so we only use 'NOx') switch
                    df.columns[16],# NH3 (More than 90% empty on some files, we drop this feature instead of entire datasets)
                    df.columns[17],# SO2 (More than 90% empty on some files, we drop this feature instead of entire datasets)
                    df.columns[20],# Benzene (10 'Benzene' columns have more than MAX_NAN_PERCENTAGE %)
                    df.columns[21],# Toluene (9 'Toluene' columns have more than MAX_NAN_PERCENTAGE %)
                    df.columns[22],# Xylene (11 'Xylene' columns have more than MAX_NAN_PERCENTAGE %)
                    df.columns[23],# MP-Xylene (31 'MP-Xylene' columns have more than MAX_NAN_PERCENTAGE %)
                    df.columns[24]]# Eth-Xylene (30 'Eth-Xylene' columns have more than MAX_NAN_PERCENTAGE %)

    df = df.drop(drop_columns, axis=1)
    # *********************************************************************************************

    # We rename columns to remain consistant in feature names
    df.columns = ['Station Name','From','FT','To','TT','PM10(ug/m3)', 'PM2.5(ug/m3)','AT()', 'BP(mmHg)',
                    'SR(W/mt2)','RH(%)', 'WD(degree)','RF(mm)','NOx(ppb)','NO2(ug/m3)','CO(mg/m3)', 'Ozone(ug/m3)']
    return df


def main():
    # Change this to the directory with the CSV data of stations
    directory = 'data/raw_data'
    # This will be the directory with all our processed data
    out = 'data/final_data'

    # The max acceptable percantage of empty data on any givien column 
    MAX_NAN_PERCENTAGE = 0.20

    # Statin names of all sets we keep
    kept_sets = []

    # We keep track of the number of columns that are over MAX_NAN_PERCENTAGE
    colums_over_max_nan = defaultdict(lambda: 0)
    total_num_sets = 0

    for filename in os.scandir(directory):
        if filename.is_file():
            total_num_sets += 1
            # For some reasone this file needs to be read one row before compared to all
            # other files
            if filename.name == 'AQ_Alipur_Aug21_July22_N.csv':
                df = pd.read_csv(filename.path, header=0)
            else:
                df = pd.read_csv(filename.path, header=1)

            # We store the station name before deleting it. We will rename the files to station the as the new filename
            station_name = df.iloc[0][0]

            # We drop un-used columns
            df = drop_columns(df)

            if keep_data_set(df, MAX_NAN_PERCENTAGE, filename, colums_over_max_nan):

                # We add additional features
                df = add_seasons(df)
                df = add_lat_lon(df, station_name)
                
                # We fill all the missing values in the df and apply three fill methods:
                # forward fill, backward fill, and forward & backward fill.
                df = fill_missing_data(df)

                os.makedirs(out, exist_ok=True)
                df.to_csv(out + "/" + station_name + ".csv", index=False)
    
                kept_sets.append(station_name + ".csv")

    print("We keep", len(kept_sets), "out of", total_num_sets, "sets.")
    print("These sets included:", sorted(kept_sets))
    

if __name__ == "__main__":
    main()