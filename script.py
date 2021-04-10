import pandas as pd
import datetime
import math
from progressbar import ProgressBar
from tqdm import tqdm
import numpy as np
import csv


def expiry(row):
    x = row['strike']
    return x[-18:-11]


def main():
    pbar = ProgressBar()

    # with open('./test_data.csv', newline='') as f:
    #     df = pd.read_csv('./test_data.csv', low_memory=False, skiprows=1,
    #                      names=['strike', 'datetime', 'open', 'high', 'low', 'close', 'expiry'])

    # df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    # df['expiry'] = pd.to_datetime(df.expiry, errors='coerce')
    # print(df)

    with open('./test_ref.csv', newline='') as f:
        df_ref = pd.read_csv('./test_ref.csv', skiprows=6)

    df_ref.rename({
        "Unnamed: 0": "date",
        "Unnamed: 1": "time",
        "Unnamed: 5": "spot_target_1",
        "Unnamed: 6": "spot_target_2",
        "Unnamed: 8": "strike",
        "Unnamed: 9": "option_type",
        "Unnamed: 10": "expiry",
        "Unnamed: 17": "exit_signal_date",
        "Unnamed: 18": "exit_signal_time"}, axis="columns", inplace=True)

    df_ref["strike"] = df_ref['strike'].str.replace('[^\w\s]', '')

    df_ref['datetime'] = pd.to_datetime(df_ref['date'] + ' ' + df_ref['time'])
    df_ref['exit_signal_datetime'] = pd.to_datetime(
        df_ref['exit_signal_date'] + ' ' + df_ref['exit_signal_time'])

    df_opt_result = pd.DataFrame(
        columns=['Strike', 'Entry Price', 'Exit at Signal', 'Exit at Target 1', 'Exit at Target 2', 'Target Datetime 1', 'Target Datetime 2'])

    for index, ref_row in df_ref.iterrows():
        if(ref_row['time'] == '15:15:00'):
            df_ref.at[index, 'datetime'] = ref_row['datetime'] + \
                datetime.timedelta(minutes=14)
        else:
            df_ref.at[index, 'datetime'] = ref_row['datetime'] + \
                datetime.timedelta(minutes=59)

        if(ref_row['exit_signal_time'] == '15:15:00'):
            df_ref.at[index, 'exit_signal_datetime'] = ref_row['exit_signal_datetime'] + \
                datetime.timedelta(minutes=14)
        else:
            df_ref.at[index, 'exit_signal_datetime'] = ref_row['exit_signal_datetime'] + \
                datetime.timedelta(minutes=59)

    # df_ref['datetime'] = pd.to_datetime(df_ref['date'] + ' ' + df_ref['time'])
    # df_ref['datetime'] = df_ref['datetime'] + datetime.timedelta(minutes=14)

    df_ref['expiry'] = pd.to_datetime(df_ref.expiry, errors='coerce')

    with open('./spot_data_test.csv', newline='') as f:
        df_spot = pd.read_csv('./spot_data_test.csv', low_memory=False, skiprows=1,
                              names=['strike', 'datetime', 'open', 'high', 'low', 'close'])
    df_spot = df_spot.dropna()
    df_spot['datetime'] = pd.to_datetime(df_spot.datetime, errors='coerce')

    print('Entering loop')

    with tqdm(total=df_ref.shape[0]) as pbar:

        for index, ref_row in df_ref.iterrows():

            pbar.update(1)
            entry_price = 0.0
            exit_signal = 0.0
            target1_datetime = 'No Target Met'
            target2_datetime = 'No Target Met'
            entry_datetime = ref_row['datetime']
            exit_datetime = ref_row['exit_signal_datetime']
            strike_ref = str(ref_row['strike'])

            df_spot_temp = df_spot.loc[(
                df_spot['datetime'] >= ref_row['datetime']) & (df_spot['datetime'] <= ref_row['exit_signal_datetime'])]

            # Finding target datetime against spot
            for index, spot_row in df_spot_temp.iterrows():

                high = float(spot_row['high'])
                low = float(spot_row['low'])
                option_type = str(ref_row['option_type'])
                spot_datetime = spot_row['datetime']
                target1 = float(ref_row['spot_target_1'])
                target2 = float(ref_row['spot_target_2'])

                if(option_type == 'CE'):
                    # if(spot_datetime >= entry_datetime and spot_datetime <= exit_datetime):
                    # print(str(high) + ': ' + str(target1))
                    if(high >= target1):
                        target1_datetime = spot_datetime

                    if(high >= target2):
                        target2_datetime = spot_datetime
                else:
                    # if(spot_datetime >= entry_datetime and spot_datetime <= exit_datetime):
                    if(low <= target1):
                        target1_datetime = spot_datetime
                    if(low <= target2):
                        target2_datetime = spot_datetime
            """
            df_temp = df.loc[(df['expiry'] == ref_row['expiry'])]

            print(' Checking for ' + str(ref_row['strike']))

            for index, data_row in df_temp.iterrows():

                data_option_type = str(data_row['strike'][-6:-4])
                strike_data = str(data_row['strike'][-11:-6])
                
                option_ref = str(ref_row['option_type'])

                if(ref_row['datetime'] == data_row['datetime'] and data_option_type == ref_row['option_type'] and strike_ref == strike_data):
                    entry_price = float(data_row['close'])
                    row = ('Entry: ' + str(entry_price),
                           strike_data, data_option_type)
                    print(row)

                if(ref_row['exit_signal_datetime'] == data_row['datetime'] and data_option_type == ref_row['option_type'] and strike_ref == strike_data):
                    exit_signal = float(data_row['close'])
                    row = ('Exit: ' + str(exit_signal),
                           strike_data, data_option_type)
                    print(row)

                if(exit_signal != 0.0 and entry_price != 0.0):
                    break
            """
            df_opt_result = print_rec(
                df_opt_result, strike_ref, entry_price, exit_signal, 0.0, 0.0, target1_datetime, target2_datetime)

    print(df_opt_result)
    df_opt_result.to_csv(r'./test_res.csv')


def target_times(ref_row, df_spot):

    for index, spot_row in df_spot.iterrows():
        target1_datetime = 'No Target Met'
        target2_datetime = 'No Target Met'
        high = spot_row['high']
        low = spot_row['low']
        option_type = str(ref_row['option_type'])
        spot_datetime = spot_row['datetime']
        entry_datetime = ref_row['datetime']
        exit_datetime = ref_row['exit_signal_datetime']
        target1 = ref_row['spot_target_1']
        target2 = ref_row['spot_target_2']

        if(option_type == 'CE'):
            if(spot_datetime >= entry_datetime and spot_datetime <= exit_datetime):
                if(high >= target1):
                    target1_datetime = spot_datetime
                if(high >= target2):
                    target2_datetime = spot_datetime
        else:
            if(spot_datetime >= entry_datetime and spot_datetime <= exit_datetime):
                if(low <= target1):
                    target1_datetime = spot_datetime
                if(low <= target2):
                    target2_datetime = spot_datetime

        res_row = [target1_datetime, target2_datetime]
        return res_row


def print_rec(df_opt_result, strike, entry_price, exit_signal, exit_target_1, exit_target_2, date_target_1, date_target_2):

    return df_opt_result.append({
                                'Strike': strike,
                                'Entry Price': entry_price,
                                'Exit at Signal': exit_signal,
                                'Exit at Target 1': exit_target_1,
                                'Exit at Target 2': exit_target_2,
                                'Target Datetime 1': date_target_1,
                                'Target Datetime 2': date_target_2}, ignore_index=True)


def round_down(x, a):
    return math.floor(x / a) * a


def round_up(x, a):
    return math.ceil(x / a) * a


def round_nearest(x, a):
    return round(x / a) * a


if __name__ == "__main__":
    main()
