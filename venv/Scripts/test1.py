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
import math

username = 'yinxiaofang@etouch.cn'
password = 'etouch2015'
app_keys = ('5a8fdf31b27b0a59ad000096', '5a8fdfc1b27b0a033f000082')
app_key_map = {'5a8fdf31b27b0a59ad000096': '91988061', '5a8fdfc1b27b0a033f000082': '91988062'}
period_type = ('daily', 'weekly', 'monthly')

DATE_FORMAT = "%Y-%m-%d"
# 将日期转换为周
def date_to_week(date_str):
    date = (datetime.datetime.strptime(date_str, DATE_FORMAT) + datetime.timedelta(6)).isocalendar()
    return '' + str(date[0]) + str(date[1])
    # return date.strftime("%Y") + date.isocalendar()[0:2]


# 获取所有的channel id
def get_all_encode_channels():
    wlkk_android_channel = {}
    wlkk_ios_channel = {}
    for app_key in app_keys:
        param = {'appkey': app_key, 'per_page': 500}
        r = requests.get("http://api.umeng.com/channels", auth=(username, password), params=param)
        print r
        if app_key == app_keys[0]:
            for channel in r.json():
                wlkk_android_channel[channel["id"]] = channel['channel']
        else:
            for channel in r.json():
                wlkk_ios_channel[channel["id"]] = channel['channel']

    return wlkk_android_channel, wlkk_ios_channel


if __name__ == '__main__':
    # print datetime.date(2018, 12, 31).isocalendar()[0:2]
    # print date_to_week("2018-07-08")
    # param = {'appkey': app_keys[0], 'start_date': '2018-07-08', 'end_date': '2018-07-08', 'per_page': 500,
    #          'period_type': period_type[1]}
    #
    # param['channel'] = 'ceshi111'
    # r = requests.get("http://api.umeng.com/active_users", auth=(username, password), params=param).json()
    # print r

    # var = 0
    # while var != '1':  # 该条件永远为true，循环将无限执行下去
    #     var = raw_input("Enter a number  :")
    #     print "You entered: ", var
    # print "Good bye!"


    # print range(5)
    # print range(2, 5)
    # print range(2, 5, 2)
    #
    #
    # print max(1, 2, 3)
    # print math.modf(-1.2)
    # print math.e
    # print math.pi

    # print (datetime.datetime.now() - datetime.timedelta(2)).isocalendar()
    #
    # range()

    # if 'a' != 'a':
    #     print '相等'
    # else:
    #     print 1
    # a = u'你好'.decode('unicode_escape')
    # print a

    # data = (123,'1,2,3')
    # print data
    # print data[0]
    # print data[1]
    # print type(data[1]) == type(u'1')
    # array = data[1].encode('unicode-escape').decode('string_escape').split(',')
    # print array
    title = u'你好'
    print u'<b>{}（符合条件的有{:d}人,占今日总新增）：</b>'.format(title,1)



