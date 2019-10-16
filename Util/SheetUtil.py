import datetime
import pymysql

def insert_Entry(values,terr):
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'insert into mass_entry(account,userName,registerDate,entryDate,ifSim,openSource,extensionSource,source,medium,sex,age,province,city,plan,unit,keyword,terraceId) values '
    now_date = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    if not values:
        print('No data found.')
    else:
        for row in values:
            if( len(row) > 5 and row[0] is not None and row[0] != ''):
                if(datetime.datetime.strptime(row[0]+'','%m/%d/%Y').strftime('%Y-%m-%d') == now_date):
                    for i in range(17-len(row)):
                        row.append('')
                    sql = sql + '("'+row[1]+'","'+row[2]+'","'+row[3]+'","'+row[4]+'","'+row[5]+'","'+row[6]+'","'+row[7]+'","'+row[8]+'","'+row[9]+'","'+row[10]+'","'+row[11]+'","'+row[12]+'","'+row[13]+'","'+row[14]+'","'+row[15]+'","'+row[16]+'",'+terr+'),'
    if sql.endswith(','):
        s = sql[:-1]
        cursor.execute(s)
        conn.commit()
        cursor.close()
        conn.close()
def insert_Register(values,terr):
    conn = pymysql.connect(host="localhost", user="root", password="12345678", database="platform", charset="utf8")
    cursor = conn.cursor()
    sql = 'insert into mass_register(registerDate,openSource,mass,source,medium,plan,unit,keyword,gaId,userAccount,' \
          'terraceId) values '
    now_date = (datetime.date.today() + datetime.timedelta(-1)).strftime('%Y-%m-%d')
    if not values:
        print('No data found.')
    else:
        for row in values:
            if len(row) > 5 and row[0] is not None and row[0] != '':
                if datetime.datetime.strptime(row[0] + '', '%Y/%m/%d').strftime('%Y-%m-%d') == now_date:
                    for i in range(11-len(row)):
                        row.append('')
                    sql = sql + '("'+row[1]+'","'+row[2]+'","'+row[3]+'","'+row[4]+'","'+row[5]+'","'+row[6]+'","'+row[7]+'","'+row[8]+'","'+row[9]+'","'+row[10]+'",'+terr+'),'
    if sql.endswith(','):
        s = sql[:-1]
        cursor.execute(s)
        conn.commit()
        cursor.close()
        conn.close()