from GoogleConfig import GoogleConfig
from GoogleDef import GaGet
import pymysql


def main():
    analytics = GoogleConfig.get_ga()
    print(111)
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'select * from mass_returncall a where a.ifCheck is null'
    registerResult = None
    try:
        cursor.execute(sql)
        registerResult = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    for rRow in registerResult:
        print(rRow[3])
        dateddd = rRow[3].strftime('%Y-%m-%d')
        terraceId = rRow[19]
        if terraceId == 1:
            viewId = '185946517'
        elif terraceId == 2:
            viewId = '162478221'
        elif terraceId == 3:
            viewId = '185946517'
        elif terraceId == 4:
            viewId = '197372064'
        if terraceId == 1:
            response = GaGet.get_user_message_by_account_igold(analytics, viewId, dateddd, rRow[18] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_igold(analytics, viewId, dateddd, (rRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})
        elif terraceId == 3:
            response = GaGet.get_user_message_by_account_acetop(analytics, viewId, dateddd, rRow[18] + '')
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[17] + '')[:-1])
                data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})
        else:
            response = GaGet.get_user_message(analytics, viewId, dateddd, (rRow[17] + '')[:-1])
            data = response.get('reports', [])[0].get('data', {})
            if isinstance(data.get('rowCount'), int) is False:
                response = GaGet.get_user_message_by_account(analytics, viewId, dateddd, rRow[18] + '')
                data = response.get('reports', [])[0].get('data', {})

        # 如果确实查不到，到库中修改为已核对
        if isinstance(data.get('rowCount'), int) is False:
            updateSql = "UPDATE mass_returncall SET ifCheck = 1 where id = '%d'" % (rRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
            continue

        # 查出来的话，就选取查出数据的第一条，判定是否为.com 或 not set ，如果是，就取另外一条，如果没有其他的，就依然取第一条
        dataRows = data.get('rowCount', int)
        trurRowNum = 0
        if dataRows > 1:
            for i in range(0, dataRows):
                checkRow = data.get('rows', [])[i].get('dimensions', [])[1]
                if checkRow.find(".com") > -1 or checkRow.find("not set") > -1 or checkRow.find("(none)") > -1:
                    continue
                else:
                    trurRowNum = i
                    break
        rowData = data.get('rows', [])[trurRowNum]
        checkData = rowData.get('dimensions', [])
        if len(checkData) > 1:
            source = checkData[1].split('/')[0].rstrip()
            medium = checkData[1].split('/')[1].lstrip()
            updateSql = "UPDATE mass_returncall SET extensionSource = '%s', source = '%s', medium = '%s', plan = '%s'," \
                        " unit = '%s', keyword = '%s', ifCheck = 1 WHERE id = %d" % (
                            checkData[1], source, medium, checkData[2], checkData[3], checkData[4], rRow[0])
            try:
                cursor.execute(updateSql)
                conn.commit()
            except:
                conn.rollback()
        print(checkData)


if __name__ == '__main__':
    main()
