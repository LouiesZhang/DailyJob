import datetime
import time

from Auto import GaGet
from Auto import logUtil
import warnings

warnings.filterwarnings('ignore')

logger = logUtil.log

# 核对注册
def check_re(analytics, conn, cursor, terraceId, viewId, t1, t2):
    try:
        sql = 'select a.id,a.register_date,a.ga_id,a.member_account from ad_register a where a.terrace_id = "%d" and a.register_date >= "%s" and a.register_date <= "%s" and a.if_check is null order by a.register_date' % (terraceId, t1, t2)
        registerResult = None
        try:
            cursor.execute(sql)
            registerResult = cursor.fetchall()
        except:
            logger.info("Error: unable to fetch data")
        if registerResult is None:
            return
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
                    accounts = accounts + rr[3] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_igold(analytics, viewId, key, accounts[:-1])
                    #处理获得的数据
                    checkRull(response, oneDayRows, conn, cursor, 3)

            elif terraceId == 3:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[3] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_acetop(analytics, viewId, key, accounts[:-1])
                    checkRull(response, oneDayRows, conn, cursor, 3)

            gaIds = ""
            for rr in oneDayRows:
                if rr[2] == "undefined.undefined":
                    continue
                gaIds = gaIds + rr[2] + "|"
            if gaIds != "":
                response = GaGet.get_user_message(analytics, viewId, key, gaIds[:-1])
                checkRull(response, oneDayRows, conn, cursor, 2)

            accounts = ""
            for rr in oneDayRows:
                accounts = accounts + rr[3] + "|"
            if accounts != "":
                response = GaGet.get_user_message_by_account(analytics, viewId, key, accounts[:-1])
                checkRull(response, oneDayRows, conn, cursor, 3)

            if len(oneDayRows) > 0:
                checkIds = ""
                for rr in oneDayRows:
                    checkIds = checkIds + str(rr[0]) + ","
                updateSql = "UPDATE ad_register SET if_check = 1 where id in (%s)" % (checkIds[:-1])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
    except:
        logger.error("ga注册核对出现问题,重新请求")
        time.sleep(1)
        check_re(analytics, conn, cursor, terraceId, viewId, t1, t2)

# 核对入金
def check_en(analytics, conn, cursor, terraceId, viewId, t1 ,t2):
    try:
        sql = 'select a.id,a.register_date,a.ga_id,a.member_account from ad_entry a where a.terrace_id = "%d" and a.entry_date >= "%s" and a.entry_date<= "%s" and a.if_check is null order by a.register_date' % (terraceId, t1, t2)
        entryResult = None
        try:
            cursor.execute(sql)
            entryResult = cursor.fetchall()
        except:
            logger.info("Error: unable to fetch data")
        daysRows = {}
        daysList = []
        if entryResult is None:
            return
        for eRow in entryResult:
            dateKey = eRow[1].strftime('%Y-%m-%d')
            daysList.append(dateKey)
        daysList = list(set(daysList))
        for daysKey in daysList:
            daysRows[daysKey] = []
        for eRow in entryResult:
            entDate = eRow[1].strftime('%Y-%m-%d')
            daysRows[entDate].append(eRow)

        for key in daysRows:
            logger.info(key)
            oneDayRows = daysRows[key]
            if terraceId == 1:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[3] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_igold(analytics, viewId, key, accounts[:-1])
                    #处理获得的数据
                    checkRullE(response, oneDayRows, conn, cursor, 3)
            elif terraceId == 3:
                accounts = ""
                for rr in oneDayRows:
                    accounts = accounts + rr[3] + "|"
                if accounts != "":
                    response = GaGet.get_user_message_by_account_acetop(analytics, viewId, key, accounts[:-1])
                    checkRullE(response, oneDayRows, conn, cursor, 3)

            gaIds = ""
            for rr in oneDayRows:
                if rr[2] == "undefined.undefined":
                    continue
                gaIds = gaIds + rr[2] + "|"
            if gaIds != "":
                response = GaGet.get_user_message(analytics, viewId, key, gaIds[:-1])
                checkRullE(response, oneDayRows, conn, cursor, 2)

            accounts = ""
            for rr in oneDayRows:
                accounts = accounts + rr[3] + "|"
            if accounts != "":
                response = GaGet.get_user_message_by_account(analytics, viewId, key, accounts[:-1])
                checkRullE(response, oneDayRows, conn, cursor, 3)

            if len(oneDayRows) > 0:
                checkIds = ""
                for rr in oneDayRows:
                    checkIds = checkIds + str(rr[0]) + ","
                updateSql = "UPDATE ad_entry SET if_check = 1 where id in (%s)" % (checkIds[:-1])
                try:
                    cursor.execute(updateSql)
                    conn.commit()
                except:
                    conn.rollback()
    except:
        logger.error("ga入金核对出现问题,重新请求")
        time.sleep(1)
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
                updateSql = "UPDATE ad_register SET mass = '%s', put_ga_name = '%s', site_ga_name = '%s', plan_ga_name = '%s', unit = '%s', keyword = '%s', if_check = 1 WHERE id = %d" % (
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
                updateSql = "UPDATE ad_entry SET mass = '%s', put_ga_name = '%s', site_ga_name = '%s', plan_ga_name = '%s'," \
                            " unit = '%s', keyword = '%s', if_check = 1 WHERE id = %d" % (
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





