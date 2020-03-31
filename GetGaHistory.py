from GoogleConfig import GoogleConfig
import pymysql
import datetime
from dateutil.relativedelta import relativedelta

viewId = '172236768'
terraceId = 2
def main():
    analytics = GoogleConfig.get_ga()
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="gahistory", charset="utf8mb4")
    cursor = conn.cursor()
    print("right")
    t1 = '2019-01-01'
    while t1 < '2020-01-01':
        t2 = (datetime.datetime.strptime(t1, '%Y-%m-%d') + relativedelta(years=+1) - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        print(t1+'~'+t2)
        response = analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': viewId,
                        'dateRanges': [{'startDate': t1, 'endDate': t2}],
                        'samplingLevel': 'LARGE',
                        'metrics': [{'expression': 'ga:sessions'}, {'expression': 'ga:users'}, {'expression': 'ga:avgSessionDuration'}],
                        'dimensions': [{'name': 'ga:sourceMedium'}, {'name': 'ga:eventCategory'}, {'name': 'ga:city'}, {'name': 'ga:operatingSystem'}, {'name': 'ga:appVersion'}],
                        'orderBys': [{'fieldName': 'ga:sessions', 'sortOrder': 'DESCENDING'}]
                    }]
            }
        ).execute()
        print(response)

        #入库,放入response 和 平台ID
        sql = 'insert into ga_his_fivedimension(date,mass,source,med,eventTab,city,device,edition,flowNum,userNum,sessionSeconds,terraceId,viewId,dateType) values '
        for report in response.get('reports', []):
            for row in report.get('data', {}).get('rows', []):
                dimensions = row.get('dimensions', [])  # 来源媒介+广告系列
                dateRangeValues = row.get('metrics', [])  # 会话数
                source = dimensions[0].split('/')[0].rstrip()
                medium = dimensions[0].split('/')[1].lstrip()
                sql = sql + '("%s","%s","%s","%s","%s","%s","%s","%s",%s,%s,%s,%d,%s,"%s"),' % (t1+"~"+t2, dimensions[0], source, medium, dimensions[1], dimensions[2], dimensions[3], dimensions[4],
                                                                     dateRangeValues[0].get("values", [])[0], dateRangeValues[0].get("values", [])[1], dateRangeValues[0].get("values", [])[2], terraceId, viewId, "year")
        if sql.endswith(','):
            s = sql[:-1]
            print(s)
            cursor.execute(s)
            conn.commit()
        t1 = (datetime.datetime.strptime(t1, '%Y-%m-%d') + relativedelta(years=+1)).strftime('%Y-%m-%d')
    cursor.close()
    conn.close()
if __name__ == '__main__':
  main()