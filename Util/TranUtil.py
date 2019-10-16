import datetime
import pymysql


def insert_massResponse(response):
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'insert into mass_log(flowDate,mass,source,flowNum,terraceId) values '
    now_date = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    for report in response.get('reports', []):
        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])  # 来源媒介+广告系列
            dateRangeValues = row.get('metrics', [])  # 会话数
            sql = sql + '("' + now_date + '","' + dimensions[0] + '","' + dimensions[1] + '",' + dateRangeValues[0].get(
                'values', [])[0] + ',4),'
    if sql.endswith(','):
        s = sql[:-1]
        cursor.execute(s)
        conn.commit()
        cursor.close()
        conn.close()
