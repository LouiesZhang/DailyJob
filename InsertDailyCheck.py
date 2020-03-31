from GoogleConfig import GoogleConfig
from GoogleDef import SheetDef
import pymysql

SAMPLE_SPREADSHEET_ID_R = '1xJOd2ZgLD73695j7ioWesefh6hF0A8NrmKrrz27CTdo'
SAMPLE_RANGE_NAME_R_BIB = 'bibgold（3月）!A1:K'
SAMPLE_RANGE_NAME_R_BIBFX = 'bibfx（3月）!A1:K'
SAMPLE_NUMBER_R_BIB = 621438978
SAMPLE_NUMBER_R_BIBFX = 241251244

SAMPLE_SPREADSHEET_ID_E = '1WG7JZMtS1IKK0oXAhMFFxaDYRc8TnUtZ3Nq_PdWho1A'
SAMPLE_RANGE_NAME_E_IGOLD = '领峰-3月!A2:S'
SAMPLE_RANGE_NAME_E_BIB = '皇御-3月!A2:S'
SAMPLE_RANGE_NAME_E_ACETOP = 'acetop-3月!A2:T'
SAMPLE_RANGE_NAME_E_BIBFX = '皇御bibfx-3月!A2:S'
SAMPLE_NUMBER_E_IGOLD = 1742986796
SAMPLE_NUMBER_E_BIB = 1141575553
SAMPLE_NUMBER_E_ACETOP = 858628966
SAMPLE_NUMBER_E_BIBFX = 1100449695

SAMPLE_SPREADSHEET_ID_X = '1b3Fe3kcyFpDYqIHyxg7ThXhKQDDvILjD3WvSSXhUBww'
SAMPLE_RANGE_NAME_R_XUAN = '注册!A2:L'
SAMPLE_RANGE_NAME_E_XUAN = '入金!A2:L'
SAMPLE_NUMBER_R_XUAN = 1516750319
SAMPLE_NUMBER_E_XUAN = 1452013795


def main():
    service = GoogleConfig.get_google_sheet()
    print("hahaha")

    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    result = None

    #bib注册
    sql = 'SELECT a.registerDate AS registerDate, `a`.`registerDate` AS `registerDate1`, `a`.`openSource` AS `openSource`, `a`.`mass` AS `mass`, `a`.`source` AS `source` , `a`.`medium` AS `medium`, `a`.`plan` AS `plan`, `a`.`unit` AS `unit`, `a`.`keyword` AS `keyword`, `a`.`gaId` AS `gaId` , `a`.`userAccount` AS `userAccount` FROM `mass_register` `a` WHERE `a`.`terraceId` = 2 AND `a`.`registerDate` >= curdate() - INTERVAL 3 DAY ORDER BY `a`.`registerDate` DESC'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%Y/%m/%d')
        rowb[1] = rowa[1].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_R, SAMPLE_NUMBER_R_BIB, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_R, SAMPLE_RANGE_NAME_R_BIB+str(len(newResult)+1), newResult)
    print(valuesw)

    #bibfx注册
    sql = 'SELECT a.registerDate AS registerDate, `a`.`registerDate` AS `registerDate1`, `a`.`openSource` AS `openSource`, `a`.`mass` AS `mass`, `a`.`source` AS `source` , `a`.`medium` AS `medium`, `a`.`plan` AS `plan`, `a`.`unit` AS `unit`, `a`.`keyword` AS `keyword`, `a`.`gaId` AS `gaId` , `a`.`userAccount` AS `userAccount` FROM `mass_register` `a` WHERE `a`.`terraceId` = 4 AND `a`.`registerDate` >= curdate() - INTERVAL 3 DAY ORDER BY `a`.`registerDate` DESC'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%Y/%m/%d')
        rowb[1] = rowa[1].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_R, SAMPLE_NUMBER_R_BIBFX, len(newResult))
    valuesw = SheetDef.insert_value(service,SAMPLE_SPREADSHEET_ID_R,SAMPLE_RANGE_NAME_R_BIBFX+str(len(newResult)+1), newResult)
    print(valuesw)

    #igold入金
    sql = 'select a.entryDate,a.account,a.userName,a.registerDate,a.entryDate,a.ifSim,a.openSource,a.extensionSource,a.source,a.`medium`,a.sex,a.age,a.province,a.city,a.plan,a.unit,a.keyword,a.gaId,a.userAccount from mass_entry a where a.terraceId = 1 and a.entryDate >= (curdate() - INTERVAL 3 DAY) order by a.entryDate desc'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%m/%d/%Y')
        rowb[3] = rowa[3].strftime('%Y/%m/%d %H:%M')
        rowb[4] = rowa[4].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_E, SAMPLE_NUMBER_E_IGOLD, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_E,
                                    SAMPLE_RANGE_NAME_E_IGOLD + str(len(newResult) + 1), newResult)
    print(valuesw)

    #bib入金
    sql = 'SELECT a.entryDate AS entryDate, `a`.`account` AS `account`, `a`.`userName` AS `userName`, `a`.`registerDate` AS `registerDate`, `a`.`entryDate` AS `entryDate1` , `a`.`ifSim` AS `ifSim`, `a`.`openSource` AS `openSource`, `a`.`extensionSource` AS `extensionSource`, `a`.`source` AS `source`, `a`.`medium` AS `medium` , `a`.`sex` AS `sex`, `a`.`age` AS `age`, `a`.`province` AS `province`, `a`.`city` AS `city`, `a`.`plan` AS `plan` , `a`.`unit` AS `unit`, `a`.`keyword` AS `keyword`, `a`.`gaId` AS `gaId`, `a`.`userAccount` AS `userAccount` FROM `mass_entry` `a` WHERE `a`.`terraceId` = 2 AND `a`.`entryDate` >= curdate() - INTERVAL 3 DAY ORDER BY `a`.`entryDate` DESC'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%m/%d/%Y')
        rowb[3] = rowa[3].strftime('%Y/%m/%d %H:%M')
        rowb[4] = rowa[4].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_E, SAMPLE_NUMBER_E_BIB, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_E,
                                    SAMPLE_RANGE_NAME_E_BIB + str(len(newResult) + 1), newResult)
    print(valuesw)

    #acetop入金
    sql = 'select a.entryDate,a.account,a.userAccount,a.userName,a.registerDate,a.entryDate,a.ifSim,a.openSource,a.extensionSource,a.source,a.`medium`,a.sex,a.age,a.province,a.city,a.plan,a.unit,a.keyword,a.gaId,a.userAccount from mass_entry a where a.terraceId = 3 and a.entryDate >= (curdate() - INTERVAL 3 DAY) order by a.entryDate desc'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%m/%d/%Y')
        rowb[4] = rowa[4].strftime('%Y/%m/%d %H:%M')
        rowb[5] = rowa[5].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_E, SAMPLE_NUMBER_E_ACETOP, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_E,
                                    SAMPLE_RANGE_NAME_E_ACETOP + str(len(newResult) + 1), newResult)
    print(valuesw)

    #bibfx入金
    sql = 'SELECT a.entryDate AS entryDate, `a`.`account` AS `account`, `a`.`userName` AS `userName`, `a`.`registerDate` AS `registerDate`, `a`.`entryDate` AS `entryDate1` , `a`.`ifSim` AS `ifSim`, `a`.`openSource` AS `openSource`, `a`.`extensionSource` AS `extensionSource`, `a`.`source` AS `source`, `a`.`medium` AS `medium` , `a`.`sex` AS `sex`, `a`.`age` AS `age`, `a`.`province` AS `province`, `a`.`city` AS `city`, `a`.`plan` AS `plan` , `a`.`unit` AS `unit`, `a`.`keyword` AS `keyword`, `a`.`gaId` AS `gaId`, `a`.`userAccount` AS `userAccount` FROM `mass_entry` `a` WHERE `a`.`terraceId` = 4 AND `a`.`entryDate` >= curdate() - INTERVAL 3 DAY ORDER BY `a`.`entryDate` DESC'
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[0] = rowa[0].strftime('%m/%d/%Y')
        rowb[3] = rowa[3].strftime('%Y/%m/%d %H:%M')
        rowb[4] = rowa[4].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_E, SAMPLE_NUMBER_E_BIBFX, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_E,
                                    SAMPLE_RANGE_NAME_E_BIBFX + str(len(newResult) + 1), newResult)
    print(valuesw)

    # 汇选注册
    sql = "select concat(MONTH(a.registerDate),'月') as monthNum,date_format(a.registerDate, '%Y-%m-%d') regDate,a.registerDate as regDatetime,b.`name` as terraceName,a.openSource as openSource,'' as realSource,'' as realMass,'' as realKeyword,a.userAccount,a.gaId,'' as arriveTime,a.mass from mass_register a LEFT JOIN mass_terrace b on a.terraceId = b.id where a.source REGEXP 'xuanhot|EasyH' and date_format(a.registerDate, '%Y-%m-%d') >= (CURDATE()-INTERVAL 3 DAY) order by a.terraceId"
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[2] = rowa[2].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_X, SAMPLE_NUMBER_R_XUAN, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_X,
                                    SAMPLE_RANGE_NAME_R_XUAN + str(len(newResult) + 1), newResult)
    print(valuesw)

    # 汇选入金
    sql = "select b.`name` as terraceName,date_format(a.entryDate, '%Y-%m-%d') as entDate,a.account as account,a.userAccount as userAccount,a.userName as userName,a.registerDate as regDatetime,a.entryDate as entDatetime,a.ifSim as ifSim,a.openSource as openSource,a.extensionSource as mass,'' as realMass,'' as realKeyword from mass_entry a LEFT JOIN mass_terrace b on a.terraceId = b.id where a.source REGEXP 'xuanhot|EasyH' and date_format(a.entryDate, '%Y-%m-%d') >= (CURDATE()-INTERVAL 3 DAY) order by a.terraceId"
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
    except:
        print("Error: unable to fetch data")
    newResult = []
    for rowa in result:
        rowb = list(rowa)
        rowb[5] = rowa[5].strftime('%Y/%m/%d %H:%M')
        rowb[6] = rowa[6].strftime('%Y/%m/%d %H:%M')
        newResult.append(rowb)
    print(newResult)
    SheetDef.new_row(service, SAMPLE_SPREADSHEET_ID_X, SAMPLE_NUMBER_E_XUAN, len(newResult))
    valuesw = SheetDef.insert_value(service, SAMPLE_SPREADSHEET_ID_X,
                                    SAMPLE_RANGE_NAME_E_XUAN + str(len(newResult) + 1), newResult)
    print(valuesw)

    cursor.close()
    conn.close()


if __name__ == '__main__':
    main()