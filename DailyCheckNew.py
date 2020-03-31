import datetime
import pymysql
from GoogleConfig import GoogleConfig
from DailyCheckDec import DGetOaData
from DailyCheckDec import DGaCheck
from Util import logUtil

def main():
    logger = logUtil.log
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from mass_terrace'
    result = None
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        logger.info("Error: unable to fetch data")
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


if __name__ == '__main__':
    main()
