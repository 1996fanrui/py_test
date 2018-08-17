#!/usr/bin/env python
# -*- coding=utf-8 -*-
import sys
import os
import time
import datetime
from optparse import OptionParser

_BEELINE_LOCATION = 'impala-shell'
_IMPALA_SERVER_ADDR = 'node1.lb.bigdata.dmp.com'
_IMPALA_DATABASE = 'prod'

yyyymmdd = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')

if __name__ == '__main__':

    parser = OptionParser()
    parser.add_option("-f", help="file url")
    parser.add_option("-d", help="date format yyyymmdd")
    parser.add_option("-t", help="hour format HH")
    parser.add_option("-s", help="show replaced sql")
    (options, args) = parser.parse_args()

    # input_sql = open(options.f, 'r')
    # sqlContent = input_sql.read()

    if options.d is not None:
        yyyymmdd = options.d

    yyyymmdd = '20180730'
    while yyyymmdd != '20180701':
        yyyymmdd = (datetime.datetime(*time.strptime(yyyymmdd, '%Y%m%d')[:3]) - datetime.timedelta(days=1)).strftime(
            '%Y%m%d')
        yyyymmdd_6 = (datetime.datetime(*time.strptime(yyyymmdd, '%Y%m%d')[:3]) - datetime.timedelta(days=6)).strftime(
            '%Y%m%d')
        print yyyymmdd
        print yyyymmdd_6
        print


