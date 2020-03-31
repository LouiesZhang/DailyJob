import datetime
import pymysql


def insert_massResponse(response, terraceId):
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8mb4")
    cursor = conn.cursor()
    sql = 'insert into mass_log(flowDate,mass,source,medium,plan,flowNum,terraceId) values '
    now_date = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    for report in response.get('reports', []):
        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])  # 来源媒介+广告系列
            dateRangeValues = row.get('metrics', [])  # 会话数
            source = dimensions[0].split('/')[0].rstrip()
            medium = dimensions[0].split('/')[1].lstrip()
            sql = sql + "('%s','%s','%s','%s','%s','%s',%d)," % (now_date, dimensions[0], source, medium, dimensions[1],
                                                                dateRangeValues[0].get('values', [])[0], terraceId)
    if sql.endswith(','):
        s = sql[:-1]
        cursor.execute(s)
        conn.commit()
        cursor.close()
        conn.close()
