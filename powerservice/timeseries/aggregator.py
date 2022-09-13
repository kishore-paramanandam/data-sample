
from powerservice import trading
import pandas as pd
from datetime import date
import datetime
from pandas_profiling import ProfileReport
import json
from pandas.io.json import json_normalize
import sys


def trigger_aggregation(path="c:/"):
    current_date = date.today()
    current_date_string = current_date.strftime('%d/%m/%Y')
    dt_string = datetime.datetime.now().strftime("%Y%m%d_%H%M")
    file_name = f"{path}/PowerPosition_{dt_string}"
    profiling_file_name = f"{file_name}_data_profiling"
    previous_date = current_date + datetime.timedelta(days=-1)
    previous_date_string = previous_date.strftime('%d/%m/%Y')

    current_date_input_list = trading.get_trades(current_date_string)
    previous_date_input_list = trading.get_trades(previous_date_string)

    current_date_input_df = pd.DataFrame(current_date_input_list[0])
    current_date_input_df = current_date_input_df.loc[(current_date_input_df['time'] < '23:00')]

    previous_date_input_df = pd.DataFrame(previous_date_input_list[0])
    previous_date_input_df = previous_date_input_df.loc[(previous_date_input_df['time'] >= '23:00')]
    df_union_current_previous_date = pd.concat([previous_date_input_df, current_date_input_df])

    aggregate_data(df_union_current_previous_date, file_name)

    prepare_data_profiling(df_union_current_previous_date, profiling_file_name)

    prepare_data_quality(df_union_current_previous_date, previous_date_input_df, current_date_input_df, file_name)

def aggregate_data(df_union_current_previous_date, file_name):
    df_filter = df_union_current_previous_date.dropna(subset=['time', 'volume'])
    df_filter['Local Time'] = df_filter['time'].str.slice(0, 2)
    df_agg = df_filter.groupby(['Local Time'], sort=False).sum('volume')
    df_agg.to_csv(f"{file_name}.csv")

def prepare_data_profiling(df_union_current_previous_date, profiling_file_name):
    profile = ProfileReport(df_union_current_previous_date, title="Petroineos Profiling Report")
    profile.to_file(f"{profiling_file_name}.json")
    with open(f"{profiling_file_name}.json", encoding='utf-8') as f_input:
        json_file = json.load(f_input)
        json_df = json_normalize(json_file)
    json_df.to_csv(f"{profiling_file_name}.csv", encoding='utf-8', index=False)

def prepare_data_quality(df_union_current_previous_date, previous_date_input_df, current_date_input_df, file_name):
    dq_dict = {}
    # Missing Values dq check
    missing_volumn = df_union_current_previous_date[df_union_current_previous_date.volume.isnull()]
    dq_dict['Missing_Values'] = missing_volumn.count()['date']
    # Start and End time dq check
    Start_time = previous_date_input_df['time'].min()
    End_time = current_date_input_df['time'].max()
    if Start_time != '23:00' or End_time != '22:55':
        dq_dict['Start_End_Time_Check'] = 'Fail'
    else:
        dq_dict['Start_End_Time_Check'] = 'Pass'
    # Time Interval dq check
    df_union_current_previous_date['Hour'] = df_union_current_previous_date['time'].str.slice(0, 2)
    Time_check = df_union_current_previous_date.groupby(['Hour'], sort=False).count()
    Time_check = Time_check.loc[Time_check['time'] != 12]
    Time_check = Time_check.count()['date']

    if Time_check > 0:
        dq_dict['Time_interval_check'] = 'Fail'
    else:
        dq_dict['Time_interval_check'] = 'Pass'
    dq_df = pd.DataFrame.from_dict(dq_dict, orient='index')
    dq_df = dq_df.transpose()
    dq_file_name = f"{file_name}_data_quality"
    dq_df.to_csv(f"{dq_file_name}.csv")


if __name__ == '__main__':
    path = sys.argv[1]
    trigger_aggregation(path)


