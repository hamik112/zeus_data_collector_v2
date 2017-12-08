#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@xxx.cn
Created Time: 2017-06-21 15:59:00
"""

from enum import Enum, unique

"""
定义Account Status的类
"""
@unique
class AccountStatus(Enum):
    ACTIVE		= 1
    DISABLED	= 2
    UNSETTLED	= 3
    CLOSED		= 101



@unique
class CampaignStatus(Enum):
    ACTIVE		= 'ACTIVE'
    PAUSED		= 'PAUSED'
    DELETED		= 'DELETED'
    ARCHIVED	= 'ARCHIVED'


@unique
class AdsetStatus(Enum):
    ACTIVE		= 'ACTIVE'
    PAUSED		= 'PAUSED'
    DELETED		= 'DELETED'
    ARCHIVED	= 'ARCHIVED'

# vim: set noexpandtab ts=4 sts=4 sw=4 :
