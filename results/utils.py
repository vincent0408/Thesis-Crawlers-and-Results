from glob import glob
from datetime import datetime
import requests
import pandas as pd
from tqdm import tqdm
from twitchAPI.twitch import Twitch
import json
from IPython.display import clear_output
import pandas as pd
import time

def send_error_to_line(msg):
    token = ''
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code

def get_hour_crawl_df_lst(path: str, origin: pd.DataFrame = None, convert_data = False) -> list:
    df = pd.read_csv(path, dtype=str, header=None)
    df_lst = []
    idx_lst = df.loc[df[0] == 'id'].index.to_list()
    idx_lst.append(df.shape[0])
    for i in range(1, len(idx_lst)):
        sep_df = df.iloc[idx_lst[i-1]:idx_lst[i]]
        sep_df = sep_df.rename(columns=sep_df.iloc[0]).drop(sep_df.index[0]).reset_index(drop=True)
        convert_type = {
            'viewer_count': int,
        }
        sep_df = sep_df.astype(convert_type)
        # if(convert_data):
        #     sep_df['started_at'] = pd.to_datetime(sep_df['started_at'], format='%y/%m/%d-%H:%M:%S')
        #     sep_df['crawl_started_at'] = pd.to_datetime(sep_df['crawl_started_at'], format='%y/%m/%d-%H:%M:%S')
        #     sep_df['crawl_ended_at'] = pd.to_datetime(sep_df['crawl_ended_at'], format='%y/%m/%d-%H:%M:%S')
        #     sep_df['is_mature'] = sep_df['is_mature'].replace({'True': True, 'False': False})
        if(origin is not None):
            sep_df = sep_df.merge(origin, left_on='user_login', right_on='user_login', how='left').fillna('')
        else:
            sep_df = sep_df.fillna('')
        df_lst.append(sep_df)
    return df_lst

# if __name__ == '__main__':
    #asyncio.run(get_user_bio(['supertf', 'xqc', 'muselk', 'loserfruit']))

def retry_n_times(func, mes, n):
    for i in range(n):
        try:
            res = func()
            return res
        except Exception as e:
            send_error_to_line(f'{mes}\n\n{e}')
            time.sleep(5)
            if(i == n - 1):
                send_error_to_line(f'{mes}\n\nTried {n-1} times still failed\n\n{e}')

def get_origin_df(origin_path):
    return pd.read_csv(origin_path+'/origin.csv', dtype=str, names=['user_login', 'origin', 'origin_request_time'])

def get_daily_origin_df_dict(daily_origin_path_lst) -> dict:
    daily_origin_df_unmod_lst = []
    daily_origin_df_modded_dict = {}
    for week_origin_path in tqdm(daily_origin_path_lst):
        daily_origin_df_unmod_lst.append(pd.read_csv(week_origin_path, dtype=str, usecols=[0, 1], names=['user_login', 'origin']))
    clear_output(wait=True)
    for i in tqdm(range(len(daily_origin_df_unmod_lst) - 1)):
        daily_origin_df_modded_dict[daily_origin_path_lst[i+1].split('/')[-1].strip('.csv')] = pd.merge(daily_origin_df_unmod_lst[i+1], daily_origin_df_unmod_lst[i], how='outer').drop_duplicates(subset='user_login')
    return daily_origin_df_modded_dict
