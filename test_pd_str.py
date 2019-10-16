import requests
import numpy as nu
import pandas as pd
from sqlalchemy import create_engine

import warnings

warnings.filterwarnings('ignore')


# 获取OA_session
def get_session(platform):
    headers = {'X-Requested-With': 'XMLHttpRequest',
               'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0'
               }
    data = {'UserName': 'Drumeng',
            'Password': 'sdfj23723sk(0',
            'RememberMe': 'false'
            }
    OA_session = requests.session()
    url = r'http://{}.acetopbms.com/System/Login'.format(platform)
    OA_session.post(url=url, data=data, headers=headers)
    return OA_session


# 取数据
def get_data(OA_session, t1, t2, platform, RorE):
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
    print(response)
    da = pd.read_html(response.text)[0]
    return da


# 删除测试数据
def del_test(da):
    da.fillna({'IP区域(省)': '', '广告来源': '',
               '会员帐号': '', '会员姓名': '', '会员昵称': ''}, inplace=True)

    # 排除台湾
    t0 = da.shape[0]
    ex0 = ['台湾', '香港']
    da = da.loc[~da['IP区域(省)'].isin(ex0)]
    t1 = da.shape[0]
    print('排除了{}条“台湾，香港”的数据'.format(t0 - t1))

    # 排除test
    t0 = da.shape[0]
    li = ['广告来源', '会员帐号', '会员姓名', '会员昵称']
    for i in li:
        da = da[~da[i].str.contains('test')].copy()
        da = da[~da[i].str.contains('TEST')].copy()
        da = da[~da[i].str.contains('Test')].copy()
        da = da[~da[i].str.contains('测试')].copy()
        da = da[~da[i].str.contains('測試')].copy()

    t1 = da.shape[0]
    print('排除了{}条“test”的数据'.format(t0 - t1))
    return da


# 整理数据
def data_clean(da, RorE, terraceId):
    da = del_test(da)
    if terraceId == 2:
        da = da[(da['交易平台'] == 'MT4') & (~da['开户来源'].str.contains('Global' or 'global'))]
    elif terraceId == 4:
        da = da[(da['交易平台'] == 'MT5') & (da['开户来源'].str.contains('Global' or 'global'))]
    # 去重
    da.drop_duplicates(subset=['会员帐号', '会员姓名'], inplace=True)
    if RorE == 0:
        da = da[['注册时间', '开户来源', '广告来源', '媒介', '广告计划', '广告单元', '关键字', 'GAID', '会员帐号']]
        da['mass'] = da['广告来源'] + r' / ' + da['媒介']
        da.columns = ['registerDate', 'openSource', 'source', 'medium', 'plan', 'unit', 'keyword', 'gaId',
                      'userAccount', 'mass']
    else:
        da = da[['交易帐号', '会员姓名', '注册时间', '开户时间', '有无模拟账号', '开户来源', '广告来源', '媒介', '性别', '年龄',
                 'IP区域(省)', 'IP区域(市)', '广告计划', '广告单元', '关键字', 'GAID', '会员帐号']]
        da['extensionSource'] = da['广告来源'] + r' / ' + da['媒介']
        da.columns = ['account', 'userName', 'registerDate', 'entryDate', 'ifSim', 'openSource', 'source', 'medium',
                      'sex', 'age', 'province', 'city', 'plan', 'unit', 'keyword', 'gaId', 'userAccount',
                      'extensionSource']
    return da


# 入库
def to_mysql(df1, RorE):
    engine = create_engine('mysql+pymysql://root:12345678@192.168.119.129:3306/platform?charset=GBK')
    if RorE == 0:
        df1.to_sql('mass_register_1', engine, index=False, if_exists='append')
    else:
        df1.to_sql('mass_entry_1', engine, index=False, if_exists='append')



def main():
    nu.set_printoptions(suppress=True)
    OA_session = get_session('bib')
    da = {
        "TradeID": "",
        "MbUserName": "B2019101420072490277",
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
    url = r'http://{}.acetopbms.com/ReportCenter/RegisterSourceReport'.format('bib')
    response = OA_session.post(url=url, data=da)
    print(response)
    da['GAID'].apply(lambda x: float(x)).astype('object')
    da = pd.read_html(response.text)[0]
    print(da["GAID"])
    df = data_clean(da, 0, 4)
    df['terraceid'] = 4
    to_mysql(df, 0)




if __name__ == '__main__':
    main()
