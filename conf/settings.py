#-*- coding: utf-8 -*-

import os
# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.9/howto/deployment/checklist/
# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = 'f8w%ue9^nq9e+4(pibr#!jyk2r%9mat1_*b5vj5!vomxbjkx#a'
# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False
ALLOWED_HOSTS = []
# Internationalization
# https://docs.djangoproject.com/en/1.9/topics/i18n/
LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'Asia/Shanghai'
USE_I18N = True
USE_L10N = True
USE_TZ = True
# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.9/howto/static-files/
STATIC_URL = '/static/'
#FB_Authentication
APPID = '596024127199279'
APPSECRET = '9b3843d6596bf1dec2d691472c5d9e18'

OUTPUTFILEPATH = '/home/liuyang/wwwroot/zeus/zeus_data_collector_v2/data'
