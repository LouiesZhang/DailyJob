import datetime
import pymysql
from Auto import GoogleConfig, SheetDef
from Auto import logUtil

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
        SheetDef.new_row(service, row[3], row[5], len(newResult))
        valuesw = SheetDef.insert_value(service, row[3],
                                        row[4] + str(len(newResult) + 1), newResult)
        logger.info(valuesw)