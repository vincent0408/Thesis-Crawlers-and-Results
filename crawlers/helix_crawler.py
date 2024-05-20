from twitchAPI.twitch import Twitch, TwitchAPIException
import asyncio
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import requests
import pandas as pd
import os, time, traceback


def write_helix_error(err_msg, sda_data_path):
    with open(sda_data_path + 'helix_error_logs.txt', 'a') as f:
        f.writelines([datetime.now().strftime('%y/%m/%d-%H:%M:%S'), '\n', err_msg, '\n'])

async def init_twitch(sda_data_path):
    # get your clien_id and clien_secret at https://dev.twitch.tv/console/apps
    client_id_1 = ''
    client_secret_1 = ''
    client_id_2 = ''
    client_secret_2 = ''

    for i in range(12):
        try:
            twitch_1 = await Twitch(client_id_1, client_secret_1)
            twitch_2 = await Twitch(client_id_2, client_secret_2)
            return twitch_1, twitch_2
        except Exception as e:
            write_helix_error(traceback.format_exc(), sda_data_path)
            send_error_to_line(
                'Can not initialize OAuth token, retrying...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%y/%m/%d-%H:%M:%S')))
            time.sleep(2)
            if(i == 11):
                send_error_to_line(
                    'Failed to initialize OAuth token after 12 retries...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%y/%m/%d-%H:%M:%S')))

async def get_streams(twitch_1, sda_data_path):
    attributes_to_keep = ['id', 'user_id', 'user_login', 'game_id', 'game_name', 'viewer_count', 'started_at', 'language', 'is_mature']
    for i in range(20):
        try:
            res_lst = []
            async for res in twitch_1.get_streams(first=100):
                res = res.to_dict()
                res_lst.append([res[attr] for attr in attributes_to_keep])
            return pd.DataFrame(res_lst, columns=attributes_to_keep).drop_duplicates(subset=['user_login'])
        except Exception as e:
            write_helix_error(traceback.format_exc(), sda_data_path)
            send_error_to_line('Can not get data from helix API, retrying...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%y/%m/%d-%H:%M:%S')))
            time.sleep(2)
            if(i == 19):
                send_error_to_line('Failed to get data from helix API after 20 retries...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%y/%m/%d-%H:%M:%S')))

async def get_ccl(streamer_id_lst, twitch2) -> pd.DataFrame:
    res_lst = []
    total_streamer_count = len(streamer_id_lst)
    bins = total_streamer_count // 100 + 1 if total_streamer_count % 100 != 0 else total_streamer_count // 100 
    for i in range(bins):
        batch = streamer_id_lst[100*i:100*(i+1)]
        try:
            for res in await twitch2.get_channel_information(batch):
                res_lst.append([res.broadcaster_id, ''.join([ccl[0] for ccl in res.content_classification_labels])])
        except TwitchAPIException as e:
            send_error_to_line(f'Twitch API Exception\n\n{e}')
            print(batch)
            batch = [s for s in batch if s.isnumeric()]
            n = 0
            while(n < 10):
                try:
                    for res in await twitch2.get_channel_information(batch):
                        res_lst.append([res.broadcaster_id, ''.join([ccl[0] for ccl in res.content_classification_labels])])
                    break
                except:
                    n += 1
                    time.sleep(1)
        except Exception as e:
            send_error_to_line(f'{total_streamer_count} streamers\n\n{e}')
            print(batch)


    return pd.DataFrame(res_lst, columns=['user_id', 'content_classification_labels'])

async def write_file_and_logs(df, start, sda_data_path, twitch2):
    last_crawl_time = datetime.now()
    df['crawl_started_at'] = start.strftime('%y/%m/%d-%H:%M:%S')
    df['crawl_ended_at'] = last_crawl_time.strftime('%y/%m/%d-%H:%M:%S')
    df['started_at'] = pd.to_datetime(df['started_at'], 
                                      format='%Y-%m-%dT%H:%M:%S%z').dt.tz_convert('Asia/Taipei').dt.strftime('%y/%m/%d-%H:%M:%S')
    df.to_csv(sda_data_path + start.strftime('%Y-%U/last_crawl.csv'), mode='w', index=False)
    try:
        df2 = await get_ccl(list(df['user_id']), twitch2)
        df3 = pd.merge(df, df2, on='user_id', how='left').fillna('')
        df3.to_csv(sda_data_path + start.strftime('%Y-%U/%y%m%d-%H.csv'), mode='a', index=False)
    except Exception as e:
        df['content_classification_labels'] = "nr"
        df.to_csv(sda_data_path + start.strftime('%Y-%U/%y%m%d-%H.csv'), mode='a', index=False)
        send_error_to_line('df3 failed, saved df instead...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%y/%m/%d-%H:%M:%S')))    

    with open(sda_data_path + start.strftime('%Y-%U/') + 'logs.txt', 'a', encoding='utf-8') as f:
        end = datetime.now()
        elapsed = str(end - start).split('.')[0]
        f.write(
            '{}, started at {}, saved last_crawl.csv at {}, ended at {}, used {}\n'.format(
                start.strftime('%y%m%d-%H%M%S'),
                start.strftime('%Y/%m/%d-%H:%M:%S'), 
                last_crawl_time.strftime('%Y/%m/%d-%H:%M:%S'), 
                end.strftime('%Y/%m/%d-%H:%M:%S'), elapsed
            )
        )

def send_error_to_line(msg):
    token = ''  # get your token at https://notify-bot.line.me/en/
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg}
    r = requests.post("https://notify-api.line.me/api/notify", headers = headers, params = payload)
    return r.status_code

async def main():
    sda_data_path = './test/'   # '/mnt/sda/tnecniv/tnecniv-2023/'
    start = datetime.now()
    print(f'Crawl started at {start.strftime("%Y/%m/%d-%H:%M:%S")}')
    os.makedirs(sda_data_path + start.strftime('%Y-%U/'), exist_ok=True)
    twitch_1, twitch_2 = await init_twitch(sda_data_path)
    df = await get_streams(twitch_1, sda_data_path)
    await write_file_and_logs(df, start, sda_data_path, twitch_2)
    print(f"Crawl ended at {datetime.now().strftime('%Y/%m/%d-%H:%M:%S')}")

if __name__ == '__main__':
    sched = BlockingScheduler(timezone='Asia/Taipei')
    sched.add_job(lambda: asyncio.run(main()), 'cron', minute='*/10',
                   max_instances=6, misfire_grace_time=60) # next_run_time=datetime.now()
    sched.start()  
