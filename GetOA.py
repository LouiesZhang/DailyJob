from selenium import webdriver
import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine

import warnings
warnings.filterwarnings('ignore')

#取cookie
def get_cookie(url):
    driver = webdriver.Chrome()
    driver.get(url)
    driver.find_element_by_name("UserName").send_keys("Drumeng")
    driver.find_element_by_name("Password").send_keys("sdfj23723sk(0")
    driver.find_element_by_class_name("sub").click()
    driver.find_element_by_xpath("//a[@target='navTab']").click()
    cookie = driver.get_cookies()
    driver.quit()
    return cookie

#构造headers
def make_headers(cookie,pf):
    session = cookie[-1]['value']
    headers = {
        'Host':'{}.acetopbms.com'.format(pf),
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:69.0) Gecko/20100101 Firefox/69.0',
        'Accept':'*/*',
        'Accept-Language':'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Accept-Encoding':'gzip, deflate',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'X-Requested-With':'XMLHttpRequest',
        'Content-Length':'534',
        'Connection':'keep-alive',
        'Referer':'http://{}.acetopbms.com/index'.format(pf),
        'Cookie':'ASP.NET_SessionId={}; atbmsoausername=Drumeng; rememberme=false'.format(session)}
    return headers

#取数据
def get_data(headers, t1, t2, paltform):
    da = {
        "TradeID":"",
        "MbUserName":"",
        "UserName":"",
        "IPAddress":"",
        "IPAreaProvince":"",
        "IPAreaCity":"",
        "AccountPlatform":"",
        "ComFrom":"Apps,OA,Atams,LivePC,LiveMobile,MCPC,MCMobile,GlobalPC,GlobalMobile,ToutiaoPC,ToutiaoMobile,IGoldHKCom,IGoldHKALink,Agent",
        "AdSource":"",
        "AdMedium":"",
        "AdPlan":"",
        "AdUnit":"",
        "AdKeyWord":"",
        "RegStartDateTime":t1,
        "RegEndDateTime":t2,
        "StartAccountDateTime":"",
        "EndAccountDateTime":"",
        "TradeStartDateTime":"",
        "TradeEndDateTime":"",
        "Tag":"",
        "AccountTypeSelectReport":"",
        "pageNum":"1",
        "orderField":"",
        "orderDirection":"asc",
        "numPerPage":"3000"}
    url = r'http://{}.acetopbms.com/ReportCenter/RegisterSourceReport'.format(platform)
    response = requests.post(url = url, headers = headers, data = da)
    da = pd.read_html(response.text)[0]
    return da

#删除‘台湾’和‘测试’
def del_test(da):
    da['IP区域(省)'] = da['IP区域(省)'].fillna(' ')
    da['广告来源'] = da['广告来源'].fillna(' ')
    da['会员帐号'] = da['广告来源'].fillna(' ')
    da['会员姓名'] = da['广告来源'].fillna(' ')
    da['会员昵称'] = da['广告来源'].fillna(' ')

    #排除台湾
    t0 = da.shape[0]
    ex0=['台湾']
    da = da.loc[~df0['IP区域(省)'].isin(ex0)]
    t1 = da.shape[0]
    print('排除了{}条“台湾”的数据'.format(t0-t1))

    #排除test
    t0 = da.shape[0]
    li = ['广告来源','会员帐号','会员姓名','会员昵称']
    for i in li:
        da = da[~da[i].str.contains('test'or'TEST'or'Test'or'测试')]

    t1 = da.shape[0]
    print('排除了{}条“test”的数据'.format(t0-t1))
    return da

#整表
def data_clean(da):
    del_test(da)
    da = da[['注册时间','开户来源','广告来源','媒介','广告计划','广告单元','关键字','GAID','会员帐号','交易帐号','交易平台','IP区域(省)','会员姓名','会员昵称']]
    da = del_test(da)
    da = da[['注册时间','开户来源','广告来源','媒介','广告计划','广告单元','关键字','GAID','会员帐号']]
    da['mass'] = da['广告来源'] + r' / ' + da['媒介']
    da.columns = ['registerDate','openSource','source','medium','plan','unit','keyword','gaId','userAccount','mass']
    return da

#入库
def to_mysql(df1):
    engine = create_engine('mysql+pymysql://root:12345678@192.168.119.129:3306/platform')
    df1.to_sql('mass_register', engine, index=False, if_exists='append')


t1 = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 00:00:00"
t2 = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 23:59:59"


#
# #1.领峰：oa01
# #2.皇御：bib
# #3.外汇：ac
#

platform = 'bib'
url = 'http://{}.acetopbms.com/'.format(platform)
cookie = get_cookie(url)

headers = make_headers(cookie,platform)
df0 = get_data(headers, t1, t2)

data4 = df0[(df0['交易平台']=='MT5')&(df0['开户来源'].str.contains('Global'or'global'))]

df4 = data_clean(data4)
df4['terraceid'] = 4

to_mysql(df4)






