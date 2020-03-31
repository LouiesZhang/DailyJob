import requests
import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymysql
from GoogleConfig import GoogleConfig
from GoogleDef import GaGet
from lxml import etree
import time

import warnings

warnings.filterwarnings('ignore')


# 获取OA_session
def get_session(platform):
    try:
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
    except:
        print(platform+'登录失败，重试')
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
        print(response)
        da = pd.read_html(response.text)[0]

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
        return da
    except:
        print(platform+'请求失败，重试')
        time.sleep(1)
        get_data(OA_session, t1, t2, platform, RorE)


# 删除测试数据
def del_test(da):
    da.fillna({'IP区域(省)': '', '广告来源': '',
               '会员帐号': '', '会员姓名': '', '会员昵称': ''}, inplace=True)

    # 排除test
    t0 = da.shape[0]
    li = ['广告来源', '会员帐号', '会员姓名', '会员昵称']
    for i in li:

        da = da[~da[i].str.contains('測試|测试|(T|t)(E|e)(S|s|X|x)(T|t|D|d)')].copy()
    t1 = da.shape[0]
    print('排除了{}条“test”的数据'.format(t0 - t1))

    # 排除台湾
    t0 = da.shape[0]
    ex0 = ['台湾', '香港']
    da = da[~((da['IP区域(省)'].isin(ex0)) & (da['会员姓名'] == ''))]
    t1 = da.shape[0]
    print('排除了{}条“台湾，香港”的数据'.format(t0 - t1))
    return da


# 整理数据
def data_clean(da, RorE, terraceId):
    da = del_test(da)
    if RorE == 1 and terraceId == 4:
        dat = da[~da['会员帐号'].duplicated(keep=False)].copy()
        engine = create_engine('mysql+pymysql://root:12345678@192.168.119.129:3306/platform?charset=GBK')
        dat = dat[['交易帐号', '会员姓名', '注册时间', '开户时间', '有无模拟账号', '开户来源', '广告来源', '媒介', '性别', '年龄',
                 'IP区域(省)', 'IP区域(市)', '广告计划', '广告单元', '关键字', 'GAID', '会员帐号', '附件状态']]
        dat['extensionSource'] = dat['广告来源'] + r' / ' + dat['媒介']
        dat.columns = ['account', 'userName', 'registerDate', 'entryDate', 'ifSim', 'openSource', 'source', 'medium',
                      'sex', 'age', 'province', 'city', 'plan', 'unit', 'keyword', 'gaId', 'userAccount',
                      'attachmentStatus', 'extensionSource']
        dat['terraceid'] = terraceId
        dat.to_sql('mass_register_bibfx_tran', engine, index=False, if_exists='append')
    if terraceId == 2:
        da = da[(da['交易平台'] == 'MT4') & (~da['开户来源'].str.contains('Global' or 'global'))]
    elif terraceId == 4:
        da = da[(da['交易平台'] == 'MT5') & (da['开户来源'].str.contains('Global' or 'global'))]
    # 去重
    da.sort_values(['注册时间', '附件状态'], ascending=[0,1], inplace=True)
    da.drop_duplicates(subset=['会员帐号', '会员姓名'], inplace=True)
    if RorE == 0:
        da = da[['注册时间', '开户来源', '广告来源', '媒介', '广告计划', '广告单元', '关键字', 'GAID', '会员帐号', '附件状态']]
        da['mass'] = da['广告来源'] + r' / ' + da['媒介']
        da.columns = ['registerDate', 'openSource', 'source', 'medium', 'plan', 'unit', 'keyword', 'gaId',
                      'userAccount', 'attachmentStatus', 'mass']
    else:
        da = da[['交易帐号', '会员姓名', '注册时间', '开户时间', '有无模拟账号', '开户来源', '广告来源', '媒介', '性别', '年龄',
                 'IP区域(省)', 'IP区域(市)', '广告计划', '广告单元', '关键字', 'GAID', '会员帐号', '附件状态']]
        da['extensionSource'] = da['广告来源'] + r' / ' + da['媒介']
        da.columns = ['account', 'userName', 'registerDate', 'entryDate', 'ifSim', 'openSource', 'source', 'medium',
                      'sex', 'age', 'province', 'city', 'plan', 'unit', 'keyword', 'gaId', 'userAccount', 'attachmentStatus',
                      'extensionSource']

    return da


# 入库
def to_mysql(df1, RorE):
    engine = create_engine('mysql+pymysql://root:12345678@192.168.119.129:3306/platform?charset=GBK')
    if RorE == 0:
        df1.to_sql('mass_register', engine, index=False, if_exists='append')
    else:
        df1.to_sql('mass_entry', engine, index=False, if_exists='append')


# 核对注册
def check_re(analytics, conn, cursor, terraceId, viewId):
    sql = 'select * from mass_register a where a.terraceId = "%d" and a.ifCheck is null' % (terraceId)
    registerResult = None
    try:
        cursor.execute(sql)
        registerResult = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    for rRow in registerResult:
        print(rRow[1])
        dateddd = rRow[1].strftime('%Y-%m-%d')
        # 如果为领峰系，先用领峰系自己的核对方式，查不到再用皇御系方式核对
        if terraceId == 1:
            response = GaGet.get_user_message_by_account_igold(analytics, viewId, dateddd, rRow[10] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_igold(analytics, viewId, dateddd, (rRow[9] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[9] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[10] + '')
                data = response.get('reports', [])[0].get('data', {})
        elif terraceId == 3:
            response = GaGet.get_user_message_by_account_acetop(analytics, viewId, dateddd, rRow[10] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[9] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[10] + '')
                data = response.get('reports', [])[0].get('data', {})
        else:
            response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[9] + '')[:-1])
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[10] + '')
                data = response.get('reports', [])[0].get('data', {})

        # 如果确实查不到，到库中修改为已核对
        if isinstance(data.get('rowCount'), int) is False:
            updateSql = "UPDATE mass_register SET ifCheck = 1 where id = '%d'" % (rRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
            continue

        # 查出来的话，就选取查出数据的第一条，判定是否为.com 或 not set ，如果是，就取另外一条，如果没有其他的，就依然取第一条
        dataRows = data.get('rowCount', int)
        trueRowNum = 0
        if dataRows > 1:
            for i in range(dataRows-1, -1, -1):
                checkRow = data.get('rows', [])[i].get('dimensions', [])[1]
                if checkRow.find(".com") > -1 or checkRow.find("not set") > -1 or checkRow.find("(none)") > -1:
                    continue
                else:
                    trueRowNum = i
                    break
        rowData = data.get('rows', [])[trueRowNum]
        checkData = rowData.get('dimensions', [])
        if len(checkData) > 1:
            source = checkData[1].split('/')[0].rstrip()
            medium = checkData[1].split('/')[1].lstrip()
            isTran = ""
            if "ref|org" in medium and terraceId == 3:
                print("is SEO")
                # 到acetop GA中核对
                response = GaGet.get_user_message_by_account_acetop_owm(analytics,'181239932', dateddd, rRow[10] + '')
                data = response.get('reports', [])[0].get('data', {})
                if isinstance(data.get('rowCount'), int) is True:
                    cRowDates = data.get('rows', [])
                    for cRowDate in cRowDates:
                        if checkRow.get('dimensions', [])[0].find("800.igoldhk.com") > -1:
                            isTran = "igold"
            updateSql = "UPDATE mass_register SET mass = '%s', source = '%s', medium = '%s', plan = '%s', unit = '%s', keyword = '%s', ifCheck = 1 WHERE id = %d" % (
                checkData[1], source, medium, checkData[2], checkData[3], checkData[4], rRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
        print(checkData)


# 核对入金
def check_en(analytics, conn, cursor, terraceId, viewId):
    sql = 'select * from mass_entry a where a.terraceId = "%d" and a.ifCheck is null' % (terraceId)
    entryResult = None
    try:
        cursor.execute(sql)
        entryResult = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    for eRow in entryResult:
        print(eRow[3])
        dateddd = eRow[3].strftime('%Y-%m-%d')

        #如果为领峰系，先用领峰系自己的核对方式，查不到再用皇御系方式核对
        if terraceId == 1:
            response = GaGet.get_user_message_by_account_igold(analytics, viewId, dateddd, eRow[18] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_igold(analytics, viewId, dateddd, (eRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (eRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, eRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})
        elif terraceId == 3:
            response = GaGet.get_user_message_by_account_acetop(analytics, viewId, dateddd, eRow[18] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (eRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, eRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})
        else:
            response = GaGet.get_user_message(analytics, viewId, dateddd, (eRow[17] + '')[:-1])
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, eRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})

        #如果确实查不到，到库中修改为已核对
        if isinstance(data.get('rowCount'), int) is False:
            updateSql = "UPDATE mass_entry SET ifCheck = 1 where id = '%d'" % (eRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
            continue

        #查出来的话，就选取查出数据的第一条，判定是否为.com 或 not set ，如果是，就取另外一条，如果没有其他的，就依然取第一条
        dataRows = data.get('rowCount', int)
        trueRowNum = 0
        if dataRows > 1:
            for i in range(dataRows-1, -1, -1):
                checkRow = data.get('rows', [])[i].get('dimensions', [])[1]
                if checkRow.find(".com") > -1 or checkRow.find("not set") > -1 or checkRow.find("(none)") > -1:
                    continue
                else:
                    trueRowNum = i
                    break
        rowData = data.get('rows', [])[trueRowNum]
        checkData = rowData.get('dimensions', [])
        if len(checkData) > 1:
            source = checkData[1].split('/')[0].rstrip()
            medium = checkData[1].split('/')[1].lstrip()
            updateSql = "UPDATE mass_entry SET extensionSource = '%s', source = '%s', medium = '%s', plan = '%s'," \
                        " unit = '%s', keyword = '%s', ifCheck = 1 WHERE id = %d" % (
                            checkData[1], source, medium, checkData[2], checkData[3], checkData[4], eRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
        print(checkData)



def main():
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from mass_terrace'
    result = None
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    analytics = GoogleConfig.get_ga()
    print("right")
    for row in result:
        OA_session = get_session(row[2])
        t1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 00:00:00"
        t2 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 23:59:59"
        for i in range(0, 2):
            df = get_data(OA_session, t1, t2, row[2], i)
            df = data_clean(df, i, row[0])
            df['terraceid'] = row[0]
            to_mysql(df, i)
            conn.commit()
            if i == 0:
                check_re(analytics, conn, cursor, row[0], row[3])
            elif i == 1:
                check_en(analytics, conn, cursor, row[0], row[3])
    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()
