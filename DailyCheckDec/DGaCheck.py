import datetime
from GoogleDef import GaGet
from Util import logUtil
import warnings

warnings.filterwarnings('ignore')

logger = logUtil.log

# 核对注册
def check_re(analytics, conn, cursor, terraceId, viewId, t1, t2):
    try:
        sql = 'select * from mass_register a where a.terraceId = "%d" and a.registerDate >= "%s" and a.registerDate<= "%s" and a.ifCheck is null order by a.registerDate' % (terraceId, t1, t2)
        registerResult = None
        try:
            cursor.execute(sql)
            registerResult = cursor.fetchall()
        except:
            logger.info("Error: unable to fetch data")
        t1Date = datetime.datetime.strptime(t1, '%Y-%m-%d %H:%M:%S')
        t2Date = datetime.datetime.strptime(t2, '%Y-%m-%d %H:%M:%S')
        days = (t2Date-t1Date).days+1
        daysRows = {}
        for day in range(days):
            dateDate = (t1Date + datetime.timedelta(days=day)).strftime('%Y-%m-%d')
            daysRows[dateDate] = []
        for rRow in registerResult:
            regDate = rRow[1].strftime('%Y-%m-%d')
            daysRows[regDate].append(rRow)

        for key in daysRows:
            logger.info(key)
            oneDayRows = daysRows[key]
            if terraceId == 1:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[10] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_igold(analytics, viewId, key, accounts[:-1])
                    #处理获得的数据
                    checkRull(response, oneDayRows, conn, cursor, 10)

            elif terraceId == 3:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[10] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_acetop(analytics, viewId, key, accounts[:-1])
                    checkRull(response, oneDayRows, conn, cursor, 10)

            gaIds = ""
            for rr in oneDayRows:
                if rr[9] == "undefined.undefined":
                    continue
                gaIds = gaIds + rr[9] + "|"
            if gaIds != "":
                response = GaGet.get_user_message(analytics, viewId, key, gaIds[:-1])
                checkRull(response, oneDayRows, conn, cursor, 9)

            accounts = ""
            for rr in oneDayRows:
                accounts = accounts + rr[10] + "|"
            if accounts != "":
                response = GaGet.get_user_message_by_account(analytics, viewId, key, accounts[:-1])
                checkRull(response, oneDayRows, conn, cursor, 10)

            if len(oneDayRows) > 0:
                checkIds = ""
                for rr in oneDayRows:
                    checkIds = checkIds + str(rr[0]) + ","
                updateSql = "UPDATE mass_register SET ifCheck = 1 where id in (%s)" % (checkIds[:-1])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
    except:
        logger.error("ga注册核对出现问题,重新请求")
        check_re(analytics, conn, cursor, terraceId, viewId, t1, t2)

# 核对入金
def check_en(analytics, conn, cursor, terraceId, viewId, t1 ,t2):
    try:
        sql = 'select * from mass_entry a where a.terraceId = "%d" and a.entryDate >= "%s" and a.entryDate<= "%s" and a.ifCheck is null order by a.registerDate' % (terraceId, t1, t2)
        entryResult = None
        try:
            cursor.execute(sql)
            entryResult = cursor.fetchall()
        except:
            logger.info("Error: unable to fetch data")
        daysRows = {}
        daysList = []
        for eRow in entryResult:
            dateKey = eRow[3].strftime('%Y-%m-%d')
            daysList.append(dateKey)
        daysList = list(set(daysList))
        for daysKey in daysList:
            daysRows[daysKey] = []
        for eRow in entryResult:
            entDate = eRow[3].strftime('%Y-%m-%d')
            daysRows[entDate].append(eRow)

        for key in daysRows:
            logger.info(key)
            oneDayRows = daysRows[key]
            if terraceId == 1:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[18] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_igold(analytics, viewId, key, accounts[:-1])
                    #处理获得的数据
                    checkRullE(response, oneDayRows, conn, cursor, 18)

            elif terraceId == 3:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[18] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_acetop(analytics, viewId, key, accounts[:-1])
                    checkRullE(response, oneDayRows, conn, cursor, 18)

            gaIds = ""
            for rr in oneDayRows:
                if rr[17] == "undefined.undefined":
                    continue
                gaIds = gaIds + rr[17] + "|"
            if gaIds != "":
                response = GaGet.get_user_message(analytics, viewId, key, gaIds[:-1])
                checkRullE(response, oneDayRows, conn, cursor, 17)

            accounts = ""
            for rr in oneDayRows:
                accounts = accounts + rr[18] + "|"
            if accounts != "":
                response = GaGet.get_user_message_by_account(analytics, viewId, key, accounts[:-1])
                checkRullE(response, oneDayRows, conn, cursor, 18)

            if len(oneDayRows) > 0:
                checkIds = ""
                for rr in oneDayRows:
                    checkIds = checkIds + str(rr[0]) + ","
                updateSql = "UPDATE mass_entry SET ifCheck = 1 where id in (%s)" % (checkIds[:-1])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
    except:
        logger.error("ga入金核对出现问题,重新请求")
        check_en(analytics, conn, cursor, terraceId, viewId, t1, t2)

def checkRull(response, oneDayRows, conn, cursor, mateNum):
    data = response.get('reports', [])[0].get('data', {})
    if isinstance(data.get('rowCount'), int) is True:
        rowDatas = data.get('rows', [])
        deleteList = []
        for rIndex, rr in enumerate(oneDayRows):
            trueRow = None
            for i, rowData in enumerate(rowDatas):
                if rr[mateNum] in rowData.get('dimensions', [])[0]:
                    checkRow = rowData.get('dimensions', [])[1]
                    if checkRow.find(".com") > -1 or checkRow.find("not set") > -1 or checkRow.find(
                            "(none)") > -1:
                        trueRow = i
                        continue
                    else:
                        trueRow = i
                        break
            if trueRow is None:
                continue
            rowData = rowDatas[trueRow]
            checkData = rowData.get('dimensions', [])
            if len(checkData) > 1:
                source = checkData[1].split('/')[0].rstrip()
                medium = checkData[1].split('/')[1].lstrip()
                updateSql = "UPDATE mass_register SET mass = '%s', source = '%s', medium = '%s', plan = '%s', unit = '%s', keyword = '%s', ifCheck = 1 WHERE id = %d" % (
                    checkData[1], source, medium, checkData[2], checkData[3], checkData[4], rr[0])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
                deleteList.append(rIndex)
            logger.info(checkData)
        deleteList.sort(reverse=True)
        for delIndex in deleteList:
            del oneDayRows[delIndex]

def checkRullE(response, oneDayRows, conn, cursor, mateNum):
    data = response.get('reports', [])[0].get('data', {})
    if isinstance(data.get('rowCount'), int) is True:
        rowDatas = data.get('rows', [])
        deleteList = []
        for rIndex, rr in enumerate(oneDayRows):
            trueRow = None
            for i, rowData in enumerate(rowDatas):
                if rr[mateNum] in rowData.get('dimensions', [])[0]:
                    checkRow = rowData.get('dimensions', [])[1]
                    if checkRow.find(".com") > -1 or checkRow.find("not set") > -1 or checkRow.find(
                            "(none)") > -1:
                        trueRow = i
                        continue
                    else:
                        trueRow = i
                        break
            if trueRow is None:
                continue
            rowData = rowDatas[trueRow]
            checkData = rowData.get('dimensions', [])
            if len(checkData) > 1:
                source = checkData[1].split('/')[0].rstrip()
                medium = checkData[1].split('/')[1].lstrip()
                updateSql = "UPDATE mass_entry SET extensionSource = '%s', source = '%s', medium = '%s', plan = '%s'," \
                            " unit = '%s', keyword = '%s', ifCheck = 1 WHERE id = %d" % (
                                checkData[1], source, medium, checkData[2], checkData[3], checkData[4], rr[0])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
                deleteList.append(rIndex)
            logger.info(checkData)
        deleteList.sort(reverse=True)
        for delIndex in deleteList:
            del oneDayRows[delIndex]





