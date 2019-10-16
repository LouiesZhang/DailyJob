from GoogleConfig import GoogleConfig
from GoogleDef import GaGet
import pymysql
import datetime

def main():
    analytics = GoogleConfig.get_ga()
    now_date = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from mass_register a where a.terraceId = 4'
    result = None
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    for row in result:
        dateddd = row[1].strftime('%Y-%m-%d')
        response = GaGet.get_user_message(analytics,'197372064',dateddd,row[9]+'')
        data = response.get('reports',[])[0].get('data', {})
        if isinstance(data.get('rowCount'), int) is False:
            response = GaGet.get_user_message_by_account(analytics, '197372064', dateddd, row[10] + '')
            data = response.get('reports', [])[0].get('data', {})
        if isinstance(data.get('rowCount'), int) is False:
            continue
        rowData = data.get('rows', [])[data.get('rowCount', int)-1]
        checkData = rowData.get('dimensions', [])
        if len(checkData) > 1:
            source = checkData[1].split('/')[0]
            medium = checkData[1].split('/')[1].lstrip()
            updateSql = "UPDATE mass_register SET mass = '%s', source = '%s', medium = '%s', plan = '%s', unit = '%s', keyword = '%s' WHERE id = %d" % (checkData[1],source,medium,checkData[2],checkData[3],checkData[4],row[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
        print(checkData)
    cursor.close()
    conn.close()
if __name__ == '__main__':
    main()
