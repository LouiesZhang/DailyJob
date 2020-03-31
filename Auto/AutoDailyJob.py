import datetime
import pymysql
from Auto import GoogleConfig, SheetDef
from Auto import DGetOaData
from Auto import DGaCheck
from Auto import logUtil
from Auto import GaGet

from apscheduler.schedulers.blocking import BlockingScheduler

#通过谷歌表格更新每日的位置素材投放变化
def getPutLog():
    logger = logUtil.log
    logger.info("获取位置投放变化：暂为完成该功能")

#获取每日媒体流量数据，精确到素材getMass done
def getMassNum():
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
    analytics = GoogleConfig.get_ga()
    logger.info("right")
    for row in result:
        # 获取数据库各平台id和GA视图ID
        # 通过视图ID获取当天流量,并拼接平台ID入库
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        response = GaGet.getMass_report(analytics, yesterday, yesterday, row[4])
        insertSql = 'insert into ad_flow_log(terrace_id,mass,put_ga_name,site_ga_name,plan_ga_name,flow_date,flow_num) values'
        # 入库,放入response 和 平台ID
        for report in response.get('reports', []):
            for rRow in report.get('data', {}).get('rows', []):
                dimensions = rRow.get('dimensions', [])  # 来源媒介+广告系列
                mass = dimensions[0]
                putGaName = dimensions[0].split('/')[0].rstrip()
                siteGaName = dimensions[0].split('/')[1].lstrip()
                planGaName = dimensions[1]
                flowNum = rRow.get('metrics', [])[0].get('values', [])[0]  # 会话数
                insertSql = insertSql + "(%d,'%s','%s','%s','%s','%s','%s')," % (row[0],mass,putGaName,siteGaName,planGaName,yesterday,flowNum)
        if insertSql.endswith(','):
            s = insertSql[:-1]
            cursor.execute(s)
            conn.commit()
        logger.info("%s 流量数据入库" % (row[1]))
    cursor.close()
    conn.close()

# 从OA获取每日注册入金并筛选DailyCheckNew
# 通过GAAPI进行核对并匹配，（todo并根据来源媒介反推其渠道，媒体，位置，素材的ID）
def dailyCheck():
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
    analytics = GoogleConfig.get_ga()
    logger.info("right")
    for row in result:
        OA_session = DGetOaData.get_session(row[2])
        t1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 00:00:00"
        t2 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d') + " 23:59:59"
        for i in range(0, 2):
            df = DGetOaData.get_data(OA_session, t1, t2, row[2], i)
            DGetOaData.data_clean(df, i, row[0])
            conn.commit()
            if i == 0:
                DGaCheck.check_re(analytics, conn, cursor, row[0], row[3], t1, t2)
            elif i == 1:
                DGaCheck.check_en(analytics, conn, cursor, row[0], row[3], t1, t2)
    cursor.close()
    conn.close()
#将每日注册入金更新到谷歌表格Insert
def dailyInsert():
    logger = logUtil.log
    service = GoogleConfig.get_google_sheet()
    logger.info("获取谷歌表格服务成功")
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="ad", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from ad_google_sheet_dict'
    result = None
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        logger.error("Error: unable to fetch data")
    for row in result:
        resultRow = None
        sql = row[8]
        try:
            cursor.execute(sql)
            resultRow = cursor.fetchall()
        except:
            logger.error("Error: unable to fetch data")
        newResult = []
        for rowa in resultRow:
            rowb = list(rowa)
            newResult.append(rowb)
        logger.info(newResult)
        if row[7] == 1:
            SheetDef.new_row(service, row[3], row[5], len(newResult))
            SheetDef.insert_value(service, row[3], row[4] + str(len(newResult) + 1), newResult)
        else:
            rangeNum = datetime.datetime.now().strftime('%d')
            tableRange = str(row[4]).replace('$', rangeNum)
            SheetDef.insert_value(service, row[3], tableRange, newResult)
        logger.info(row[1]+'-'+row[2]+'插入完成')

#定时执行以上任务
def autoJob():
    getPutLog()
    getMassNum()
    dailyCheck()
    dailyInsert()

def main():
    # logger = logUtil.log
    # logger.info("进入定时任务")
    # scheduler = BlockingScheduler()
    # scheduler.add_job(autoJob(), 'cron', day_of_week='0-6', hour=8, minute=45)
    # scheduler.start()
    # logger.info("定时任务已开启")
    # getMassNum()
    #dailyCheck()
    dailyInsert()

if __name__ == '__main__':
    main()