# -*- encoding:utf-8 -*-
import logging

import sys
from optparse import OptionParser
import datetime
import requests
from pyspark import SparkContext
from pyspark.sql import HiveContext

username = 'yinxiaofang@etouch.cn'
password = 'etouch2015'
app_keys = ('5a8fdf31b27b0a59ad000096', '5a8fdfc1b27b0a033f000082')
app_key_map = {'5a8fdf31b27b0a59ad000096': 91988061, '5a8fdfc1b27b0a033f000082': 91988062}
period_type = ('daily', 'weekly', 'monthly')

DATE_FORMAT = "%Y-%m-%d"


def date_str_2_second(user_time):
    if len(user_time) <= 0:
        return 0

    return int(user_time.split(':')[0]) * 60 + int(user_time.split(':')[1])


# 获取所有的channel id
def get_all_encode_channels():
    wlkk_android_channel = {}
    wlkk_ios_channel = {}
    for app_key in app_keys:
        param = {'appkey': app_key, 'per_page': 500}
        r = requests.get("http://api.umeng.com/channels", auth=(username, password), params=param)

        if app_key == app_keys[0]:
            for channel in r.json():
                wlkk_android_channel[channel["id"]] = channel['channel']
        else:
            wlkk_ios_channel[channel["id"]] = channel['channel']

    return wlkk_android_channel, wlkk_ios_channel


def parse_extract_data(date):
    stat_data = {}
    for app_key in app_keys:
        param = {'appkey': app_key, 'per_page': 500, 'date': date}
        r = requests.get("http://api.umeng.com/channels", auth=(username, password), params=param)

        for channel in r.json():
            active = channel['active_user']
            if 'duration' not in channel:
                logging.error("%s的数据暂时无法查询，只能统计到昨天及以前的相关的数据", date)
                continue
            duration = date_str_2_second(channel['duration'])
            channel_name = channel['channel']
            stat_data[channel['id']] = [app_key_map[app_key], date, channel_name, active, duration]

    return stat_data


def add_extend_data(date, stat_data):
    wlkk_and, wlkk_ios = get_all_encode_channels()
    retention_rate = 0.0
    new_user = 0

    for channel in wlkk_and.keys():
        add_extend(channel, date, new_user, retention_rate, stat_data, wlkk_and)

    for channel in wlkk_ios.keys():
        add_extend(channel, date, new_user, retention_rate, stat_data, wlkk_and)

    return stat_data


def add_extend(channel, date, new_user, retention_rate, stat_data, wlkk_and):
    params = {'appkey': app_keys[0], "start_date": date, "end_date": date, "period_type": period_type[0],
              'channels': channel}
    r = requests.get("http://api.umeng.com/new_users", params=params, auth=(username, password))
    if "data" in r.json():
        new_user = r.json()["data"][wlkk_and[channel]][0]
    r_retain = requests.get("http://api.umeng.com/retentions", params=params, auth=(username, password))
    if len(r_retain.json()) > 0:
        if isinstance(r_retain.json(), list):
            if "retention_rate" in r_retain.json()[0]:
                # 每个渠道每一天的次日留存
                retention_rate = r_retain.json()[0]["retention_rate"][0]
    if channel in stat_data:
        stat_data[channel].append(new_user)
        stat_data[channel].append(int((retention_rate/100) * new_user))


def validate_date_str(date_str):
    try:
        datetime.datetime.strptime(date_str, DATE_FORMAT)
        return True
    except ValueError:
        return False


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-d", help='date format yyyy-mm-dd')
    (options, args) = parser.parse_args()
    biz_date = ""

    if options.d is not None and validate_date_str(options.d):
        biz_date = options.d
    elif options.d is not None and not validate_date_str(options.d):
        logging.error("日期格式错误:yyyy-mm-dd")
        sys.exit(0)
    else:
        # 默认昨天的数据
        biz_date = (datetime.datetime.now() - datetime.timedelta(1)).strftime(DATE_FORMAT)
        print(biz_date)

    date = biz_date
    s1 = parse_extract_data(date)
    d1 = add_extend_data(date, s1)
    d2 = {k: v for k, v in d1.items() if len(v) > 5}

    sql = '''
    '''
    for data in d2.values():
        dt = datetime.datetime.strptime(data[1], DATE_FORMAT)
        sql += '''
       select %d as app_key
          ,  '%s' as channel
          ,  %d as active
          ,  %d  as new_user
          ,  %d as retain1
          ,  %d as per_duration
          ,  %s  as ds
          union all''' % (data[0], data[2], data[3], data[5], data[6], data[4], dt.strftime("%Y%m%d"))

    # 前天的数据
    date = (datetime.datetime.now() - datetime.timedelta(2)).strftime(DATE_FORMAT)
    s1 = parse_extract_data(date)
    d1 = add_extend_data(date, s1)
    d2 = {k: v for k, v in d1.items() if len(v) > 5}

    for data in d2.values():
        dt = datetime.datetime.strptime(data[1], DATE_FORMAT)
        sql += '''
       select %d as app_key
          ,  '%s' as channel
          ,  %d as active
          ,  %d  as new_user
          ,  %d as retain1
          ,  %d as per_duration
          ,  %s as ds union all''' % (data[0], data[2], data[3], data[5], data[6], data[4], dt.strftime("%Y%m%d"))

    print(sql[:-10])

    sc = SparkContext(appName="umeng_stat_load_2_hive")
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sqlContext.sql(sql[:-10]).repartition(1).write.mode("overwrite").partitionBy("ds").saveAsTable("prod.ads_hive_weili_umeng_api_stat_1d")

