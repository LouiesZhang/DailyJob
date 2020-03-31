from GoogleConfig import GoogleConfig
from GoogleDef import GaGet
from Util import TranUtil
import pymysql
import datetime

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
        #获取数据库各平台id和GA视图ID
        #通过视图ID获取当天流量,并拼接平台ID入库
        t1 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        t2 = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        response = GaGet.getMass_report(analytics, t1, t2, row[3])
        #入库,放入response 和 平台ID
        TranUtil.insert_massResponse(response, row[0])
    cursor.close()
    conn.close()
if __name__ == '__main__':
  main()