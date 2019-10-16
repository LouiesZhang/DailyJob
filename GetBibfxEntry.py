import time

from GoogleConfig import GoogleConfig
from GoogleDef import SheetDef
from Util import SheetUtil

SAMPLE_SPREADSHEET_ID = '1V8hiRCujS8l8hM36insehkJaHPfAmnRK1tn2pkCD3nM'
SAMPLE_RANGE_NAME = '皇御bibfx-9月!A2:Q'
SAMPLE_SPREADSHEET_ID_R = '1bTQmEzGz489ZH8IfZYnVTA-1NxF1B21c2aWLbVjzKLE'
SAMPLE_RANGE_NAME_R = 'bibfx开户（9月）!A2:K'
SAMPLE_RANGE_NAME_W = 'bibfx开户（9月）!A283:B284'
def main():
    service = GoogleConfig.get_google_sheet()
    values = SheetDef.get_filter_value(service,SAMPLE_SPREADSHEET_ID,SAMPLE_RANGE_NAME).get('values', [])
    SheetUtil.insert_Entry(values, '4')
    # valuesr = SheetDef.get_filter_value(service,SAMPLE_SPREADSHEET_ID_R,SAMPLE_RANGE_NAME_R).get('values', [])
    # SheetUtil.insert_Register(valuesr,'4')
    # valuesw = SheetDef.insert_value(service,SAMPLE_SPREADSHEET_ID_R,SAMPLE_RANGE_NAME_W,[['1', '2'], ['3', '4']])
    # print(valuesw)
if __name__ == '__main__':
    main()