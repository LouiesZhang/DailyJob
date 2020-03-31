import requests
import pandas as pd
from sqlalchemy import create_engine
from lxml import etree
from Auto import logUtil
import time

import warnings

warnings.filterwarnings('ignore')

logger = logUtil.log
# 获取OA_session,如果异常，重新请求
def get_session(platform):
    try:
        headers = {'X-Requested-With': 'XMLHttpRequest',
                   'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0'
                   }
        data = {'UserName': 'Drumeng',
                'Password': 'sdfj23723sk(0',
                'RememberMe': 'false'
                }
        url = r'http://{}.acetopbms.com/System/Login'.format(platform)
        OA_session = requests.session()
        OA_session.post(url=url, data=data, headers=headers)
        return OA_session
    except:
        logger.info(platform+'登录失败，重试')
        time.sleep(1)
        get_session(platform)


# 取数据
def get_data(OA_session, t1, t2, platform, RorE):
    try:
        da = {
            "TradeID": "",
            "MbUserName": "",
            "UserName": "",
            "IPAddress": "",
            "IPAreaProvince": "",
            "IPAreaCity": "",
            "AccountPlatform": "",
            "ComFrom": "",
            "AdSource": "",
            "AdMedium": "",
            "AdPlan": "",
            "AdUnit": "",
            "AdKeyWord": "",
            "RegStartDateTime": "",
            "RegEndDateTime": "",
            "StartAccountDateTime": "",
            "EndAccountDateTime": "",
            "TradeStartDateTime": "",
            "TradeEndDateTime": "",
            "Tag": "",
            "AccountTypeSelectReport": "",
            "pageNum": "1",
            "orderField": "",
            "orderDirection": "asc",
            "numPerPage": "3000"}
        if RorE == 0:
            da["RegStartDateTime"] = t1
            da["RegEndDateTime"] = t2
        else:
            da["StartAccountDateTime"] = t1
            da["EndAccountDateTime"] = t2
        url = r'http://{}.acetopbms.com/ReportCenter/RegisterSourceReport'.format(platform)
        response = OA_session.post(url=url, data=da)
        logger.info(response)
        da = pd.read_html(response.text)[0]
        #保存完整GAID
        tree = etree.HTML(response.text)
        li = tree.xpath(r'//tr[@target="TradeId"]/td/text()')
        li_1 = [[]]
        i = 0
        while len(li) > 0:
            if li[0] == '是' or li[0] == '否':
                i += 1
                li_1.append([])
            li_1[i].append(li[0])
            li.pop(0)
        li_2 = []
        for j in li_1:
            li_2.append(j[2])
        li_2.pop(0)
        da['GAID'] = pd.DataFrame(li_2)
        # 填充空白字段为空字符串
        da.fillna({'IP区域(省)': '', '广告来源': '',
                   '会员帐号': '', '会员姓名': '', '会员昵称': ''}, inplace=True)
        return da
    except:
        logger.info(platform+'请求失败，重试')
        time.sleep(1)
        return get_data(OA_session, t1, t2, platform, RorE)

# 删除测试数据并去重
def del_test(da, terraceId):
    # 排除test
    t0 = da.shape[0]
    li = ['广告来源', '会员帐号', '会员姓名', '会员昵称']
    for i in li:
        da = da[~da[i].str.contains('測試|测试|(T|t)(E|e)(S|s|X|x)(T|t|D|d)')].copy()
    t1 = da.shape[0]
    logger.info('排除了{}条“test”的数据'.format(t0 - t1))
    # 排除台湾
    t0 = da.shape[0]
    ex0 = ['台湾', '香港']
    da = da[~((da['IP区域(省)'].isin(ex0)) & (da['会员姓名'] == ''))]
    t1 = da.shape[0]
    logger.info('排除了{}条“台湾，香港”的数据'.format(t0 - t1))
    return da


# 整理字段顺序及名称并入库
def data_clean(da, RorE, terraceId):
    da = del_test(da, terraceId)
    engine = create_engine('mysql+pymysql://root:12345678@192.168.119.129:3306/ad?charset=utf8')
    if RorE == 1 and terraceId == 4:
        dat = da[~da['会员帐号'].duplicated(keep=False)].copy()
        dat = dat[['交易帐号', '交易平台','会员帐号', '会员姓名', '会员昵称','附件状态', '附件状态(银行卡)', '附件状态(身份证正面)', '附件状态(身份证背面)', '附件审核备注','注册时间', '有无模拟账号', '开户时间', '开户来源', '推广来源', '广告来源', '媒介', '广告计划', '广告单元', '关键字',
                   '性别', '年龄', '国籍', '学历', '职业', '会员状态', '风控状态', '账号状态', 'IP地址', 'IP区域(省)', 'IP区域(市)', '是否与其他账号IP相同', '相同IP数目', 'GAID']]
        dat['推广来源'] = dat['广告来源'] + r' / ' + dat['媒介']
        dat.columns = ['trade_account','trade_place','member_account','user_name','nick_name','attachment_status','attachment_bankcard','attachmentId_card_f','attachmentId_card_b','attachment_remark','register_date','if_sim', 'entry_date','open_source','mass','put_ga_name','site_ga_name','plan_ga_name','unit','keyword','sex','age','nationality','education','job','member_status','risk_status','account_status','ip','province','city','if_same_ip','same_ip_num','ga_id']
        dat['terrace_id'] = terraceId
        dat.to_sql('ad_register_bibfx_tran', engine, index=False, if_exists='append')
    if terraceId == 2:
        da = da[(da['交易平台'] == 'MT4') & (~da['开户来源'].str.contains('Global' or 'global'))]
    elif terraceId == 4:
        da = da[(da['交易平台'] == 'MT5') & (da['开户来源'].str.contains('Global' or 'global'))]
    # 去重
    da.sort_values(['注册时间', '附件状态'], ascending=[0,1], inplace=True)
    da.drop_duplicates(subset=['会员帐号', '会员姓名'], inplace=True)
    da = da[['交易帐号', '交易平台', '会员帐号', '会员姓名', '会员昵称', '附件状态', '附件状态(银行卡)', '附件状态(身份证正面)', '附件状态(身份证背面)', '附件审核备注',
             '注册时间', '有无模拟账号', '开户时间', '开户来源', '推广来源', '广告来源', '媒介', '广告计划', '广告单元', '关键字',
             '性别', '年龄', '国籍', '学历', '职业', '会员状态', '风控状态', '账号状态', 'IP地址', 'IP区域(省)', 'IP区域(市)', '是否与其他账号IP相同',
             '相同IP数目', 'GAID']]
    da['推广来源'] = da['广告来源'] + r' / ' + da['媒介']
    da.columns = ['trade_account', 'trade_place', 'member_account', 'user_name', 'nick_name', 'attachment_status',
                  'attachment_bankcard', 'attachmentId_card_f', 'attachmentId_card_b', 'attachment_remark',
                  'register_date', 'if_sim', 'entry_date', 'open_source', 'mass', 'put_ga_name', 'site_ga_name',
                  'plan_ga_name', 'unit', 'keyword', 'sex', 'age', 'nationality', 'education', 'job',
                  'member_status', 'risk_status', 'account_status', 'ip', 'province', 'city', 'if_same_ip',
                  'same_ip_num', 'ga_id']
    da['terrace_id'] = terraceId
    if RorE == 0:
        da.to_sql('ad_register', engine, index=False, if_exists='append')
    else:
        da.to_sql('ad_entry', engine, index=False, if_exists='append')
