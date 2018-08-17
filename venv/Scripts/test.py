# -*- encoding:utf-8 -*-
import logging

import sys
from optparse import OptionParser
import datetime
import requests
from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.types import (StructType, StructField, DataType, StringType, IntegerType)

username = 'yinxiaofang@etouch.cn'
password = 'etouch2015'
app_keys = ('5a8fdf31b27b0a59ad000096', '5a8fdfc1b27b0a033f000082')
app_key_map = {'5a8fdf31b27b0a59ad000096': '91988061', '5a8fdfc1b27b0a033f000082': '91988062'}
period_type = ('daily', 'weekly', 'monthly')
key_value = []

DATE_FORMAT = "%Y-%m-%d"


# 判断输入的日期参数是否合法
def validate_date_str(date_str):
    try:
        datetime.datetime.strptime(date_str, DATE_FORMAT)
        return True
    except ValueError:
        return False


# 将日期转换为周
def date_to_week(date_str):
    date = (datetime.datetime.strptime(date_str, DATE_FORMAT) + datetime.timedelta(6)).isocalendar()
    return str(date[0]) + str(date[1])


# 获取所有的channel id
def get_all_encode_channels():
    wlkk_android_channel = {}
    wlkk_ios_channel = {}
    for i in range(1, 6):
        for app_key in app_keys:
            param = {'appkey': app_key, 'per_page': 90, 'page': i}
            r = requests.get("http://api.umeng.com/channels", auth=(username, password), params=param)

            if app_key == app_keys[0]:
                for channel in r.json():
                    wlkk_android_channel[channel["id"]] = channel['channel']
            else:
                for channel in r.json():
                    wlkk_ios_channel[channel["id"]] = channel['channel']

    return wlkk_android_channel, wlkk_ios_channel


# 获取指定日期范围内所有channel的wau
def get_all_channel_wau(start_date,end_date):
    channels = get_all_encode_channels()
    wlkk_android_channel = channels[0]
    wlkk_ios_channel = channels[1]

    for app_key in app_keys:
        param = {'appkey': app_key, 'start_date': start_date, 'end_date': end_date, 'per_page': 90,
                 'period_type': period_type[1]}
        if app_key == app_keys[0]:
            for channel in wlkk_android_channel.values():
                param['channel'] = channel
                requests_get(param=param)
        else:
            for channel in wlkk_ios_channel.values():
                param['channel'] = channel
                requests_get(param=param)


# 发出请求，将数据以tuple的形式，保存到list中
def requests_get(param):
    r = requests.get("http://api.umeng.com/active_users", auth=(username, password), params=param).json()
    # print r
    if "dates" in r:
        for index in range(len(r["dates"])):
            week = date_to_week(r["dates"][index])
            wau = r["data"]["all"][index]
            key_value.append((week, param['channel'], wau))


def list_to_row(x):
    return Row(week=x[0], channel=x[1], wau=x[2])


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
        # 每周一调度，-8天，默认上周日的数据
        biz_date = (datetime.datetime.now() - datetime.timedelta(8)).strftime(DATE_FORMAT)
    print(biz_date)

    start_date = biz_date   # 回滚时,要修改的日期
    end_date = biz_date     # 回滚时,要修改的日期
    get_all_channel_wau(start_date=start_date, end_date=end_date)

    conf = SparkConf().setAppName(value="umeng_stat_wau")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    hiveContext = HiveContext(sc)
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    hiveContext.setConf("spark.sql.shuffle.partitions", "1")
    rdd1 = sc.parallelize(key_value, 1)
    rowRdd = rdd1.map(list_to_row)
    schema = StructType([StructField("week", StringType(), True),
                         StructField("channel", StringType(), True),
                         StructField("wau", IntegerType(), True)])
    dfRequest = hiveContext.createDataFrame(data=rdd1, schema=schema)
    dfRequest.registerTempTable("umeng_wau")
    hiveContext.sql("use prod")
    hiveContext.sql("insert overwrite table ads_hive_weili_umeng_api_wau_stat_1w select channel, wau, week from umeng_wau")
    sc.stop()
