# Thesis-Crawlers-and-Results
I recorded some *Tutorial Videos*, which may help understanding the code and usage.

[ssh login and Twitch data location](https://youtu.be/-39Zo2kNJss)\
[brief introduciton on usage and results](https://youtu.be/4GHOwzDkVVY)\
[executing each script](https://youtu.be/pujqppMHNxE)


# Crawlers

## Helix Crawler
The Helix Crawler fetches data from the [Get Streams](https://dev.twitch.tv/docs/api/reference/#get-streams) and [Get Channel Information](https://dev.twitch.tv/docs/api/reference/#get-channel-information) endpoints. The results are combined and stored in csv format for readability. To run it with default parameters, simply execute `python3 helix_crawler.py`

### File Structure
Each csv file contains the 6 crawls from the respective hour, this is to save the amount of files but extra splitting in real-time is inevitably needed. The parent folder is named after the week number (`%Y-%U/`), check the [documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior) for more details.
Each week, ~12GB is used for the Helix dataset.

```
./2024-19/
├── 240505-00.csv
├── 240505-01.csv
├── ...
├── 240511-23.csv
├── last_crawl.csv
└── logs.txt
```
### Data Format
All datetime related values have the format `%y/%m/%d-%H:%M:%S`, and converted to `UTC+8` already. Again, check the [documentation](https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior) for more details.

|id|user_id|user_login|game_id|game_name|viewer_count|started_at|language|is_mature|crawl_started_at|crawl_ended_at|content_classification_labels|
|-|-|-|-|-|-|-|-|-|-|-|-|
|41289840967|65653595|roger9527|138585|Hearthstone|8919|24/05/11-21:42:34|zh|False|24/05/11-23:00:00|24/05/11-23:03:51|GPSV|
#### content_classification_labels
|Content Classification Label|Abbreviation|
|:-|-|
|Mature-Rated Game|M|
|Gambling|G|
|Profanity or Vulgarity|P|
|Violent and Graphic Depictions|V|
|Sexual Themes|S|
|Drugs, Intoxication, or Excessive Tobacco Use|D|

## Usher Crawler

The Usher Crawler gets data from none other than Usher itself. The process of fetching the **Master Playlist (.m3u8)** has been well-implemented in third-party video players such as [streamlink](https://github.com/streamlink/streamlink), and of course previous NSLAB works such as [Kukudy](https://github.com/hy-chou/kukudy). Run it with `python3 usher_crawler.py`

To implement the JavaScript asynchronous behavior used in Kukudy, `asyncio` and `aiohttp` were used. The default freqency is 25Hz, allowing the crawler to get 150,000 origins per 10 minutes. The origin of a streamer is recorded once per day (UTC+8), and the streamer list is updated every 10 minutes.

At the start of each instance, there is a spinlock checking if `lock` exists. If so, it indicates that the last instance has not finished its exectution, and the new instance should pause to avoid race condition. As for the last/old instance, it checks if another instance exists whenever it records a new origin, this is done by visiting the private variable `._executors['default']._instances['nslab']` which was not meant to be used by the public, and the behavior may change as `apscheduler` updates. When a newer instance is detected, the instance saves its results and removes `lock`, freeing the newe instance from the spinlock.

### File Structure
Each day, ~25M is used for the Usher dataset.

```
./daily_origin/
├── 240516.csv
├── 240517.csv
├── ...
├── fetch_logs_240516.txt
├── fetch_logs_240517.txt
├── ...
└── lock
```
### Data Format
|Name|Origin|Fetched at|
|-|-|-|
|kaicenat|cmh01|24/05/17-00:08:00|

## Interactions
The connection between Helix and Usher crawler is the streamer list. It is attainable from the `Get Streams` endpoint, and is needed to fetch data from both `Get Channel Information` endpoint and Usher. The most recent fetch from `Get Streams` is stored as `last_crawl.csv`, allowing Usher crawler to get the most recent streamer list.

## Category Ratings Crawler

The Category Ratings Crawler is used to get the category ratings of Twitch. It returns a Boolean value indicating whether the input category is intended for mature audiences.

It is more of a script rather than a program, and is used via pasting the code into the browser console. The list of category names should be separated by `|` as shown in the example, this is to avoid [CORS](https://developer.mozilla.org/en-US/docs/Web/HTTP/CORS) using the most naive, yet effective way possible. Other minor details can be seen in the comments of the code.


# Results

These are the scripts used to generate the plots or statistics used in the thesis. 

## Jupyter Notebooks
|File Name|Usage Description|
|:-|:-|
|conformity_21Nov|Check the conformity ratio using the is_mature value from 2021 Nov~Dec dataset|
|conformity_23Mar|Check the conformity ratio using the is_mature value from 2023 Feb~Mar dataset|
|conformity_ccl|Check the conformity ratio using the Content Classification Labels from 2024 Mar dataset|
|lang_popularity|Plot the language popularity of each continent|
|ol_filter_23Aug|Successfully separate Europe and America user distribution using the (origin, language) filter|
|ol_filter_current|Applying the (origin, language) filter on more recent data|
|twitch_korea_exit|Case study of Twitch leaving the Korean market with (origin, language) filtering and mature content analysis|
|week_category_popularity|get the category popularity of selected week(s)|

## Python File
|File Name|Usage Description|
|:-|:-|
|utils|Commonly used functions, including separation of the 6 crawl csv file into list of dataframes and merging the origin back to the stream it belongs.| 

# Improvements and Future Work

Several improvements could have been made, but due to the lack of need or "If the code works, don't touch it!", these improvements have yet to be implemented, and could potentially improve the quailty for some code segments.

1. As mentioned earlier (also in the tutorial video), `apscheduler` was used mainly for backward compatibility, specifically the usage of `BlockingScheduler`. Performing asynchronous tasks in a blocking environment is weird enough on its own, not to mention the private variable had to be accessed in order to get the amount of instances. Changing the scheduler to `AsyncScheduler` or just another package may help.

2. Getting the Master Playlist from Usher is a simple process when the stream itself is in fact, using the basic streaming settings. However, some streams are unique, and additional measures had to be applied to prevent Usher simply sending back an error message. Take DRM protected streams as an example, an additional `'cdm': 'wv'` in the GET parameters is needed when requesting DRM protected streams, but this is just one of the many errors that I did managed to solve. \
On average, 98%+ of the streams can be fetched using the current code, but for the remaining 1%, it is still worthwhile to get their Master Playlist. One of the errors occurs when requesting subscriber-only streams, which when one tries to view it via browser, should give you a 6 minute preview, allowing us to get the Master Playlist this way. But due to not being a subscriber, GQL servers block you from getting the sPAToken when executing the Usher crawler. The major difference is that Usher crawler does not require a user account, therefore cannot receive the 6 minute preview benefit. How this issue can be solved requires more effort.\
These streams can attract many viewers within a specific time period, e.g. the CPBL (中華職棒大聯盟) related channels can attract up to 7k viewers when streaming baseball games during 1830 ~ 2200 on weekdays, and 1700 ~ 2030 on weekends, accounting for ~10% of Chinese viewer count.
