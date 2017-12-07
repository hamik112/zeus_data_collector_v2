#-*- coding: utf-8 -*-

import time
import argparse
import os,sys,commands
import tarfile
from django.conf import settings

class AirPort(object):
    '''
    对shell airport进行的封装
    '''
    @classmethod
    def run_airport_cmd(cls, topic, dt):
        # cd $airplane_dir
        # plane -t $topic -p 0 *
        airport_dir = '%s/%s' % (settings.OUTPUTFILEPATH, topic)
        os.chdir(airport_dir)
        # 先压缩为tar.gz，再发送
        tar_file_name = '%s.tar.gz' % (dt)
        with tarfile.open(tar_file_name, "w:gz") as tar:
            tar.add(dt, arcname=os.path.basename(dt))
        cmd = 'plane -t %s -p 0 %s' % (topic, tar_file_name)
        print(cmd)
        (status, output) = commands.getstatusoutput(cmd)
        os.remove(tar_file_name)
        return (status, output)


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='domob dsa ad sync')
    ap.add_argument("-tp", "--topic", type=str, help="the topic job you want to run, \
               zeus_ad_breakdown_data		: ad breakdown data,\
               zeus_ad_all_data			: ad all data",
                    default="zeus_ad_all_data")
    args = ap.parse_args()

    basepath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(basepath + '/../conf')
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
    status, output = AirPort.run_airport_cmd(args.topic, time.strftime('%Y%m%d', time.localtime(time.time() - 3600 * 24)))
    if status != 0:
        print 'airport meet error! status = %d, output = %s' % (status, output)
