#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-01 15:33:25
"""

import os
import sys
import time
import json
import logging
import requests
import urlparse
import pandas as pd

from facebook_interface import FacebookInterface
from multiprocessing import Process, JoinableQueue, cpu_count

class BaseCollector(object):
    def __init__(self, conf, args):
        self.app_id		= conf.get('Facebook_Authentication', 'appid')
        self.app_secret	= conf.get('Facebook_Authentication', 'app_secret')
        self.bf_access_token = conf.get('Facebook_Authentication', 'bf_access_token')
        self.dm_access_token = conf.get('Facebook_Authentication', 'dm_access_token')
        self.api_version= conf.get('Facebook_API_version', 'version')
        self.data_path	= conf.get('Path', 'data_path')
        self.fb_api		= FacebookInterface(self.app_id, self.app_secret, self.bf_access_token)
        self.topic		= args.topic
        self.timestamp	= int(time.mktime(time.strptime(args.timestamp, '%Y-%m-%d %H:%M:%S')))

        #这里是获取脚本执行前一天的数据，所以都需要减一天
        yesterday_time = time.localtime(self.timestamp - 3600*24)
        self.start_dt   = time.strftime('%Y-%m-%d', yesterday_time)
        self.stop_dt    = time.strftime('%Y-%m-%d', yesterday_time)
        self.year		= time.strftime('%Y', yesterday_time)
        self.month		= time.strftime('%m', yesterday_time)
        self.week		= time.strftime('%w', yesterday_time)
        self.dt			= time.strftime('%Y%m%d', yesterday_time)
        self.class_name	= self.__class__.__name__
        #先这样定义下company_id吧，想出好方法后再优化
        self.company_info = {self.bf_access_token:10001, self.dm_access_token:10002}
        self.account_queue	= JoinableQueue()
        self.logger		= logging.getLogger('common')
        self.wflogger	= logging.getLogger('common')


    """
    根据access_token获取相应的business id
    """
    def get_bmid_by_access_token(self, access_token):
        retry = 0
        while retry < 3:
            try:
                bm_id_dict = {}
                response = requests.get(
                    'https://graph.facebook.com/%s/me/businesses' % (self.api_version),
                    params={'access_token':access_token}
                )
                ret = json.loads(response.text)
                self.logger.info('[%s] [%s] access_token:%s, ret:%s' % (self.class_name, 'get_bmid_by_access_token', access_token, ret))
                if ret:
                    for item in ret['data']:
                        bm_id_dict[item['id']]	= access_token
                    self.logger.info('[%s] [%s] response:%s, bm_id_dict:%s' % \
                        (self.class_name, 'get_bmid_by_access_token', response, bm_id_dict))
                    return bm_id_dict
                raise Exception('Response json load Exception!')
            except Exception, why:
                retry += 1
                self.wflogger.exception('[%s] [%s] Exception, access_token:%s, response:%s, reason:%s, retry:%d' % \
                    (self.class_name, 'get_bmid_by_access_token', access_token, response, why, retry))
        return False



    """
    根据bm id 获取对应的account id, 加入到队列中
    """
    def get_accounts_by_bmid(self, business_id, access_token, next_url=''):
        retry = 0
        while retry < 3:
            try:
                if not next_url:
                    response = requests.get(
                        'https://graph.facebook.com/%s/%s/owned_ad_accounts' % (self.api_version, business_id),
                        params={'access_token':access_token}
                    )
                else:
                    response = requests.get(next_url)
                ret = json.loads(response.text)
                self.logger.info('[%s] [%s] business_id:%s, access_token:%s, next_url:%s, ret:%s' % \
                    (self.class_name, 'get_accounts_by_bmid', business_id, access_token, next_url, ret))
                if ret:
                    for key, item in ret.iteritems():
                        if key == 'data' and item != '':
                            for account_info in item:
                                self.account_queue.put((account_info['account_id'].strip(), business_id))
                        elif key == 'paging' and 'next' in item:
                            next_url = self.rebuild_request_params(item['next'], {'limit':'1000'})
                            self.get_accounts_by_bmid(business_id, access_token, next_url)
                    return True
                else:
                    raise Exception('Response json load Exception!')
            except Exception, why:
                retry += 1
                self.wflogger.exception('[%s] [%s] Exceptioin, business_id:%s, access_token:%s, next_url:%s, why:%s, retry:%d' % \
                    (self.class_name, 'get_accounts_by_bmid', business_id, access_token, next_url, why, retry))
        return False


    """
    重组url参数，主要是对limit进行重写
    """
    def	rebuild_request_params(self, url, params):
        try:
            result = urlparse.urlsplit(url)
            urlQuery = urlparse.parse_qs(result.query, True)
            for query in params:
                queryTmpList = []
                queryTmpList.append(params[query])
                urlQuery[query] = queryTmpList
            paramsList = []
            for query in urlQuery:
                queryValue = urlQuery[query]
                param = "%s=%s" % (query, queryValue[0].encode("utf-8"))
                paramsList.append(param)
            urlQuery = '&'.join(paramsList)
            rebuildUrl = urlparse.urlunsplit((result.scheme, result.netloc, result.path, urlQuery, result.fragment))
            self.logger.info('[%s] [rebuild_request_params] url:%s, params:%s, rebuildUrl:%s' % \
                (self.class_name, url, params, rebuildUrl))
            return rebuildUrl
        except Exception, why:
            self.wflogger.exception('[%s] [rebuild_request_params] Exception, url:%s, params:%s, reason:%s' % \
                (self.class_name, url, params, why))
            return url


    """
    格式化数据，将Facebook返回小数进行四舍五入并保留两位小数，由于数据库中只存入整数，进行100掊扩大后保存
    """
    def formatDecimalNumber(self, decimal_number):
        rounded_number = float("%.2f" % decimal_number)
        return int(rounded_number*100)


    """
    保存数据方法，接收列表，将数据保存为csv文件
    """
    def save_list_data(self, data, header, file):
        try:
            data_pd = pd.DataFrame(data)
            data_pd.to_csv(file, index=False, sep=',', header=header)
            del data
            del data_pd
            return True
        except Exception, why:
            self.wflogger.exception('[%s] [%s] Exception, data:%s, header:%s, file:%s' % \
                (self.class_name, 'save_list_data', data, header, file))
            del data
            return False

# vim: set noexpandtab ts=4 sts=4 sw=4 :
