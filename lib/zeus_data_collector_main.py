#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-05 14:12:04
"""

import argparse
import datetime
import os, sys
import time
import signal
import logging
import logging.config
import ConfigParser
from airport.airport import AirPort

basepath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(basepath + '/lib')


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description = 'xxx dsa ad sync')
    ap.add_argument('-d', '--executeDir', type = str,
            help = 'app execution directory',
            default = basepath)
    ap.add_argument("-t", "--timestamp", type = str,
            help = "",
            default = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    ap.add_argument("-tp", "--topic", type = str, help = "the topic job you want to run, \
            zeus_ad_breakdown_data		: ad breakdown data,\
            zeus_ad_all_data			: ad all data",
            default = "zeus_ad_all_data")

    args = ap.parse_args()
    print 'zeus_offlinedata_collector run at %s' % args.executeDir
    os.chdir(args.executeDir)
    sys.path.append(args.executeDir+'/conf')

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")

    #加载日志配置文件
    logging.config.fileConfig(args.executeDir + '/conf/logging.cfg')

    #加载配置文件
    confPath = os.path.join(args.executeDir + '/conf/zeus_offlinedata.cfg')
    conf = ConfigParser.RawConfigParser()
    conf.read(confPath)

    if args.topic == 'zeus_ad_breakdown_data':
        from collector.adcreative.adcreative_breakdown_collector import AdCreativeBreakdownCollector
        ad_creative_breakdown = AdCreativeBreakdownCollector(conf, args)
        print ('Begin to run creative breakdonw Collector for request start dt %s', args.timestamp)
        ad_creative_breakdown.run()
    elif args.topic == 'zeus_ad_all_data':
        from collector.adcreative.adcreative_all_collector import AdCreativeAllCollector
        ad_creative_all = AdCreativeAllCollector(conf, args)
        print ('Begin to run creative all Collector for request start dt %s', args.timestamp)
        ad_creative_all.run()
    else:
        print 'Do not hit the exits topics!!! Job not run'
        sys.exit(1)

    print 'starts to airport'
    status , output = AirPort.run_airport_cmd(args.topic, time.strftime('%Y%m%d', time.localtime(time.time() - 3600*24)))
    if status != 0:
        print 'airport meet error! status = %d, output = %s'%(status, output)

# vim: set noexpandtab ts=4 sts=4 sw=4 :
