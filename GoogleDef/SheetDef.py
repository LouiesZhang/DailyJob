def get_sheet_value(service, spreadsheetId, range):
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheetId,
                                range=range).execute()
    values = result.get('values', [])
    return values


def get_filter_value(service, spreadsheetId, range):
    sheet = service.spreadsheets().values()
    # range = {
    # 'data_filters': [
    #     {
    #         'developerMetadataLookup': {
    #             'metadataValue': '9/9/2019',
    #             'locationType': 'ROW'
    #         }
    #     }
    # ],
    # 'majorDimension': 'ROWS'
    # }
    # result = sheet.batchGetByDataFilter(spreadsheetId=spreadsheetId, body=range).execute()
    result = sheet.get(spreadsheetId=spreadsheetId, range=range).execute()
    return result


def insert_value(service, spreadsheetId, range, body):
    sheet = service.spreadsheets().values()
    body = {
        'values': [['1', '2'], ['3', '4']]
    }
    result = sheet.append(spreadsheetId=spreadsheetId, range=range,valueInputOption='RAW', body=body).execute()
    return result
