#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Author: liuyang@domob.cn
Created Time: 2017-06-01 11:49:12
Desc: Facebook Sdk Api
"""


from facebookads import FacebookSession
from facebookads import FacebookAdsApi
from facebookads.specs import ObjectStorySpec, LinkData, AttachmentData
from facebookads.adobjects.adaccount import AdAccount
from facebookads.adobjects.campaign import Campaign
from facebookads.adobjects.adset import AdSet
from facebookads.adobjects.ad import Ad
from facebookads.adobjects.adcreative import AdCreative


class FacebookInterface(object):
    def __init__(self, app_id, app_secret, access_token):
        self.app_id			= app_id
        self.app_secret		= app_secret
        self.access_token	= access_token
        self.fb_session		= FacebookSession(self.app_id, self.app_secret, self.access_token)
        self.fb_api			= FacebookAdsApi(self.fb_session)
        """ 设置一个默认的API对象 """
        FacebookAdsApi.set_default_api(self.fb_api)


    """
    获取Account insight数据
    @params	account_id string
    @params	breakdown_attribute string (如指定，则获取为breakdown数据)
    @params start_dt, stop_dt string 时间范围
    """
    def get_account_insight(self, account_id, breakdown_attribute=None, start_dt=None, stop_dt=None):
        account = AdAccount('act_%s' % str(account_id))
        params = {}
        if breakdown_attribute:	params['breakdowns'] = breakdown_attribute
        if start_dt and stop_dt:	params['time_range'] = {'since':start_dt, 'until':stop_dt}
        params['fields'] = [
            'account_id',
            'account_name',
            'action_values',
            'clicks',
            'impressions',
            'reach',
            'spend',
            'actions',
            'cpc',
            'cpm',
            'cpp',
            'ctr',
            'frequency'
        ]
        params['level'] = 'account'
        insights = account.get_insights(params=params)
        if insights:
            return insights
        return {}


    """
    根据Account获取Campaigns 数据
    @params	account_id string
    @params	breakdown_attribute string (如指定，则获取为breakdown数据)
    @params start_dt, stop_dt string 时间范围
    """
    def get_campaign_insights_by_account(self, account_id, breakdown_attribute=None, start_dt=None, stop_dt=None):
        account = AdAccount('act_%s' % str(account_id))
        params = {}
        if breakdown_attribute:	params['breakdowns'] = breakdown_attribute
        if start_dt and stop_dt:	params['time_range'] = {'since':start_dt, 'until':stop_dt}
        params['fields'] = [
            'account_id',
            'account_name',
            'campaign_id',
            'campaign_name',
            'action_values',
            'clicks',
            'impressions',
            'reach',
            'spend',
            'actions',
            'cpc',
            'cpm',
            'cpp',
            'ctr',
            'frequency'
        ]
        params['level'] = 'campaign'
        insights = account.get_insights(params=params)
        if insights:
            return insights
        return {}


    """
    根据campaign获取adset insights数据
    @params	campaign_id string
    @params	breakdown_attribute string (如指定，则获取为breakdown数据)
    @params start_dt, stop_dt string 时间范围
    """
    def get_adset_insight_by_campaign(self, campaign_id, breakdown_attribute=None, start_dt=None, stop_dt=None):
        campaign = Campaign('%s' % str(campaign_id))
        params = {}
        if breakdown_attribute:	params['breakdowns'] = breakdown_attribute
        if start_dt and stop_dt:	params['time_range'] = {'since':start_dt, 'until':stop_dt}
        params['fields'] = [
            'account_id',
            'account_name',
            'campaign_id',
            'campaign_name',
            'adset_name',
            'adset_id',
            'action_values',
            'clicks',
            'impressions',
            'reach',
            'spend',
            'actions',
            'cpc',
            'cpm',
            'cpp',
            'ctr',
            'frequency'
        ]
        params['level'] = 'adset'
        insights = campaign.get_insights(params=params)
        if insights:
            return insights
        return {}



    """
    获取Account的Business信息
    """
    def get_account_business_info(self, account_id):
        account = AdAccount('act_%s' % str(account_id))
        account.remote_read(fields=[AdAccount.Field.business])
        business_info = account[AdAccount.Field.business]
        if business_info:
            return business_info
        return {}


    """
    获取Account的详细信息
    """
    def get_account_info(self, account_id):
        account = AdAccount('act_%s' % str(account_id))
        account.remote_read(fields=[
            AdAccount.Field.id,
            AdAccount.Field.name,
            AdAccount.Field.account_status
        ])
        if account: return account
        return {}


    """
    获取Campaigns信息
    """
    def get_campaigns_info_by_account(self, account_id):
        account = AdAccount('act_%s' % str(account_id))
        campaigns = account.get_campaigns(fields=[
            Campaign.Field.id,
            Campaign.Field.name,
            Campaign.Field.objective,
            Campaign.Field.status,
            Campaign.Field.updated_time
        ])
        if campaigns: return campaigns
        return {}



    """
    获取Adsets信息
    """
    def get_adsets_info_by_campaign(self, campaign_id):
        campaign = Campaign('%s' % str(campaign_id))
        adsets = campaign.get_ad_sets(fields=[
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.status,
        ])
        if adsets: return adsets
        return {}



    """
    获取Account行业类型
    """
    def get_account_category(self, account_id):
        account_category_dict = {"category":"unknow", "subcategory":"unknow"}
        account = AdAccount('act_%s' % str(account_id))
        application = account.get_applications(fields=["category", "subcategory"])
        if application:
            if "category" in application[0].keys():
                account_category_dict["category"]		= application[0]["category"]
            if "subcategory" in application[0].keys():
                account_category_dict["subcategory"]	= application[0]["subcategory"]
        return account_category_dict



    """
    通过account id获取adset信息
    """
    def get_adset_info_by_account(self, account_id):
        account = AdAccount('act_%s' % str(account_id))
        adsets = account.get_ad_sets(fields=[
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.status
        ])

        if adsets: return adsets
        return {}


    """
    通过campaign id获取adset信息
    """
    def get_adset_info_by_campaign(self, campaign_id):
        campaign = Campaign('%s' % str(campaign_id))
        adsets = campaign.get_ad_sets(fields=[
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.campaign_id,
            AdSet.Field.status
        ])

        if adsets: return adsets
        return {}


    """
    获取adset breakdown数据
    """
    def get_adset_insights(self, adset_id, breakdown_attribute=None, start_dt=None, stop_dt=None):
        adsets = AdSet('%s' % (adset_id))
        params = {}
        if breakdown_attribute: params['breakdowns'] = breakdown_attribute
        if start_dt and stop_dt: params['time_range'] = {'since':start_dt, 'until':stop_dt}
        params['fields'] = [
                'account_id',
                'account_name',
                'campaign_id',
                'campaign_name',
                'adset_id',
                'adset_name',
                'ad_id',
                'ad_name',
                'buying_type',
                'spend',
                'clicks',
                'impressions',
                'cpc',
                'cpm',
                'cpp',
                'ctr',
                'actions',
                'frequency'
            ],
        params['level'] = 'adset'
        insights = adsets.get_insights(params=params)
        if insights : return insights
        return {}



    """
    通过account id获取ads信息
    """
    def get_ads_info_by_account(self, account_id):
        account = AdAccount('act_%s' % str(account_id))
        ads	= account.get_ads(fields=[
            Ad.Field.id,
            Ad.Field.name,
            Ad.Field.adset_id,
            Ad.Field.campaign_id,
            Ad.Field.status,
        ])
        if ads : return ads
        return {}


    """
    通过campaign id获取ads信息
    """
    def get_ads_info_by_campaign(self, campaign_id):
        campaign = Campaign('%s' % str(campaign_id))
        ads	= campaign.get_ads(fields=[
            Ad.Field.id,
            Ad.Field.name,
            Ad.Field.adset_id,
            Ad.Field.campaign_id,
            Ad.Field.status,
        ])
        if ads : return ads
        return {}


    """
    通过adset id获取ads信息
    """
    def get_ads_info_by_adset(self, adset_id):
        adset = AdSet('%s' % str(adset_id))
        ads	= adset.get_ads(fields=[
            Ad.Field.id,
            Ad.Field.name,
            Ad.Field.adset_id,
            Ad.Field.campaign_id,
            Ad.Field.status,
        ])
        if ads : return ads
        return {}


    """
    获取ad的 insights数据
    """
    def get_ad_insights(self, ad_id, breakdown_attribute=None, start_dt=None, stop_dt=None):
        ad = Ad('%s' % (ad_id))
        params = {}
        if breakdown_attribute:	params['breakdowns'] = breakdown_attribute
        if start_dt and stop_dt:	params['time_range'] = {'since':start_dt, 'until':stop_dt}
        params['fields'] = [
            'account_id',
            'account_name',
            'campaign_id',
            'campaign_name',
            'adset_id',
            'adset_name',
            'ad_id',
            'ad_name',
            'buying_type',
            'spend',
            'clicks',
            'impressions',
            'cpc',
            'cpm',
            'cpp',
            'ctr',
            'actions',
            'frequency'
        ]
        params['level'] = 'ad'
        insights = ad.get_insights(params=params)
        if insights : return insights
        return {}


    """
    通过creative id获取创意信息
    """
    def get_adcreative_by_id(self, creative_id):
        creatives = AdCreative('%s' % (creative_id))
        creatives.remote_read(fields=[
            AdCreative.Field.id,
            AdCreative.Field.name,
            AdCreative.Field.image_hash,
            AdCreative.Field.image_url,
            AdCreative.Field.object_type,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.status
        ])
        if creatives: return creatives
        return {}


    """
    通过ad id获取创意信息
    """
    def get_adcreative_by_adid(self, ad_id):
        ad = Ad('%s' % (ad_id))
        creatives = ad.get_ad_creatives(fields=[
            AdCreative.Field.id,
            AdCreative.Field.name,
            AdCreative.Field.image_hash,
            AdCreative.Field.image_url,
            AdCreative.Field.object_type,
            AdCreative.Field.object_story_id,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.status
        ])
        if creatives: return creatives
        return {}

# vim: set noexpandtab ts=4 sts=4 sw=4 :
