import re, csv, json, os, argparse
from datetime import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import pandas as pd
import traceback
import asyncio
import aiohttp

async def get_streamer_origin(ord, user_login, buffer, stats, sleep_time):
    await asyncio.sleep(ord * sleep_time)
    async with aiohttp.ClientSession() as session:
        url_gql = 'https://gql.twitch.tv/gql'
        data_gql = json.dumps({
            'operationName': 'PlaybackAccessToken_Template',
            'query':
            '''
            query PlaybackAccessToken_Template($login: String!, 
            $isLive: Boolean!, $vodID: ID!, $isVod: Boolean!, $playerType: String!) 
            {  streamPlaybackAccessToken(channelName: $login, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) 
            @include(if: $isLive) {    value    signature    __typename  }  
            videoPlaybackAccessToken(id: $vodID, params: {platform: "web", playerBackend: "mediaplayer", playerType: $playerType}) 
            @include(if: $isVod) {    value    signature    __typename  }}
            ''',     
            'variables': {
                'isLive': 'true',
                'login': user_login,
                'isVod': 'false',
                'vodID': '',
                'playerType': 'site',
            },
        }, separators=(',', ':'))
        headers_gql = { 'Client-ID': 'kimne78kx3ncx6brgo4mv6wki5h1ko' }
        for i in range(30):
            try:
                async with session.post(url_gql, data=data_gql, headers=headers_gql) as response:
                    result_gql = await response.text()
                    sPAToken = json.loads(result_gql)['data']['streamPlaybackAccessToken']
                    params_usher = {
                            'token': sPAToken['value'],
                            'sig': sPAToken['signature'],
                            'cdm': 'wv'
                        }                
                break
            except Exception as e:
                with open(args.sda_data_path + 'usher_error_logs.txt', 'a') as f:
                    f.writelines([datetime.now().strftime('%y/%m/%d-%H:%M:%S'), '\n', traceback.format_exc(), '\n'])                    
                # await send_error_to_line('Can not get {}\'s sPAToken from GQL, retrying...\n\n{}\n\n{}'.format(user_login, e, datetime.now().strftime('%Y/%m/%d-%H:%M:%S')), session)
                if(i == 29):
                    # await send_error_to_line(f'Failed to get {user_login}\'s sPAToken from GQL after {i+1} retries...', session)
                    return
                await asyncio.sleep(2)

        url_usher = f'https://usher.ttvnw.net/api/channel/hls/{user_login}.m3u8'

        for i in range(30):
            try:
                async with session.get(url_usher, params=params_usher) as response:
                    m3u8 = await response.text()
                break
            except Exception as e:
                with open(args.sda_data_path + 'usher_error_logs.txt', 'a') as f:
                    f.writelines([datetime.now().strftime('%y/%m/%d-%H:%M:%S'), '\n', traceback.format_exc(), '\n'])                   
                # await send_error_to_line('Can not get {}\'s m3u8 from Usher, retrying...\n\n{}\n\n{}'.format(user_login, e, datetime.now().strftime('%Y/%m/%d-%H:%M:%S')), session)
                if(i == 29):
                    await send_error_to_line(f'Failed to get {user_login}\'s m3u8 from Usher after {i+1} retries...', session)
                    return
                await asyncio.sleep(2)
        try:
            origin = re.search('ORIGIN="(.*?)"', m3u8).group(1)
            buffer.append([user_login, origin, datetime.now().strftime('%y/%m/%d-%H:%M:%S')])
            stats[1] += 1
        except:
            stats[0] += 1
            # error = re.search('"error":"(.*?)"', m3u8).group(1)

async def save_streamer_origin(start):
    week_folder_path = f"{args.sda_data_path}{start.strftime('%Y-%U')}/"
    daily_origin_path = f"{args.sda_data_path}daily_origin/"
    stream_origin_csv_path = daily_origin_path + f"{start.strftime('%y%m%d')}.csv"
    try:
        saved_streamer_name = set(pd.read_csv(stream_origin_csv_path, usecols=[0], names=["Streamer"], dtype = str)['Streamer'])
    except:
        saved_streamer_name = set()

    last_crawl_path = week_folder_path + 'last_crawl.csv'
    try:
        hour_live_streamers = pd.read_csv(last_crawl_path, usecols=[2], dtype=str)['user_login']
    except Exception as e:
        hour_live_streamers = pd.Series([])
        async with aiohttp.ClientSession() as session:
            await send_error_to_line('Cannot find last_crawl.csv when fetching origin...\n\n{}\n\n{}'.format(e, datetime.now().strftime('%Y/%m/%d-%H:%M:%S')), session)
    unrecorded_streamers = hour_live_streamers[~hour_live_streamers.isin(saved_streamer_name)].unique()
    statistics = [0, 0] # missed, new
    write_buffer = []
    max_iter = min(int(60 * args.time_interval * args.execute_frequency), len(unrecorded_streamers))
    sleep_time = 1 / min(args.execute_frequency, max_iter / 60 / args.time_interval / 0.9)    # Adjust freqeuncy
    background_tasks = set()
    
    for order in range(max_iter):
        streamer = unrecorded_streamers[order]
        task = asyncio.create_task(
            get_streamer_origin(order, streamer, write_buffer, statistics, sleep_time))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)

    try:
        await asyncio.gather(*background_tasks)
    except :
        pass
    
    with open(daily_origin_path + f'fetch_logs_{start.strftime("%y%m%d")}.txt', 'a', encoding='utf-8') as f:
        f.writelines('missed {} streamers, added {} new streamers, started at {}, ended at {}\n'.format(statistics[0], statistics[1], start.strftime("%H%M%S"), datetime.now().strftime("%H%M%S")))
    with open(stream_origin_csv_path, 'a+', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(write_buffer)
    os.remove(daily_origin_path + 'lock')
    print(f'Fetch ended at {datetime.now().strftime("%Y/%m/%d-%H:%M:%S")}')



async def send_error_to_line(msg, sess):
    token = ''  # get your token at https://notify-bot.line.me/en/
    headers = {
        "Authorization": "Bearer " + token, 
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    payload = {'message': msg}
    url = "https://notify-api.line.me/api/notify"
    await sess.post(url, headers = headers, params = payload)

def main():
    start = datetime.now()
    lock_path = f"{args.sda_data_path}daily_origin/lock"
    while(scheduler._executors['default']._instances['nslab'] > 1 or os.path.exists(lock_path)): pass
    with open(lock_path, 'w') as f: pass
    print(f'Fetch started at {start.strftime("%Y/%m/%d-%H:%M:%S")}')
    asyncio.run(save_streamer_origin(start))

def prepare_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--time_interval', type=int, default=10)  # minutes
    parser.add_argument('--execute_frequency', type=float, default=25)    # Hz
    parser.add_argument('--sda_data_path', type=str, default='/mnt/sda/tnecniv/tnecniv-2023/')
    args = parser.parse_args()
    return args

if(__name__ == '__main__'):
    args = prepare_args()
    scheduler = BlockingScheduler(timezone='Asia/Taipei')
    scheduler.add_job(lambda: main(),'cron', minute=f'8/{args.time_interval}', max_instances=2, next_run_time=datetime.now(), id='nslab', misfire_grace_time=20)
    scheduler.start() 
