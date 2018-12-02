# -*- encoding:utf-8 -*-
import logging

import sys
from optparse import OptionParser
import datetime
import requests



DATE_FORMAT = "%Y-%m-%d"

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
    r = requests.get("http://nn1.hadoop.bigdata.dmp.com:50070/webhdfs/v1/nginx-log/custom-event/v2/2018-11-14/16", params=param).json()
    print r
    # if "dates" in r:
    #     for index in range(len(r["dates"])):
    #         week = date_to_week(r["dates"][index])
    #         wau = r["data"]["all"][index]
    #         key_value.append((week, param['channel'], wau))


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
    param = {'OP':'LISTSTATUS'}

    requests_get(param=param)



