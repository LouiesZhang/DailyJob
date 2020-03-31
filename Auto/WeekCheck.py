import datetime
import pymysql
from Auto import DGetOaData
from Auto import logUtil

def weekCheck():
    logger = logUtil.log
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="ad", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from ad_terrace_dict'
    result = None
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        logger.error("Error: unable to fetch data")
    logger.info("更新核对每周数据")
    for row in result:
        OA_session = DGetOaData.get_session(row[2])
        t1 = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d') + " 00:00:00"
        t2 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 23:59:59"
        # t1 = "2020-03-01 00:00:00"
        # t2 = "2020-03-17 23:59:59"
        df = DGetOaData.get_data(OA_session, t1, t2, row[2], 0)
        da = DGetOaData.del_test(df, row[0])
        # 去重
        if row[0] == 2:
            da = da[(da['交易平台'] == 'MT4') & (~da['开户来源'].str.contains('Global' or 'global'))]
        elif row[0] == 4:
            da = da[(da['交易平台'] == 'MT5') & (da['开户来源'].str.contains('Global' or 'global'))]
        da.sort_values(['注册时间', '附件状态'], ascending=[0, 1], inplace=True)
        da.drop_duplicates(subset=['会员帐号', '会员姓名'], inplace=True)
        da.fillna({'交易帐号': '', '附件状态': '',
                   '附件状态(银行卡)': '', '附件状态(身份证正面)': '', '附件状态(身份证背面)': '', '附件审核备注': '', '开户时间': ''}, inplace=True)
        for daRow in da.itertuples():
            tradeAccount = ''
            if daRow[2] != '':
                tradeAccount = int(daRow[2])
            entryDate = "Null"
            if daRow[14] != '':
                entryDate = "'"+daRow[14]+"'"
            updateSql = "update ad_register set trade_account = '%s',user_name = '%s',nick_name = '%s',attachment_status = '%s',attachment_bankcard = '%s',attachmentId_card_f = '%s',attachmentId_card_b = '%s',attachment_remark = '%s',entry_date = %s,member_status = '%s',account_status = '%s' where member_account = '%s'" % (
            tradeAccount, daRow[5], daRow[6], daRow[7], daRow[8], daRow[9], daRow[10], daRow[11], entryDate, daRow[27], daRow[29], daRow[4])
            print(updateSql)
            try:
                cursor.execute(updateSql)
            except:
                logger.error("%s update出错" % (daRow[4]))
                continue
        conn.commit()
    cursor.close()
    conn.close()

def main():
    weekCheck()

if __name__ == '__main__':
    main()