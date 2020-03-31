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


def insert_value(service, spreadsheetId, range, appendValue):
    sheet = service.spreadsheets().values()
    body = {
        'values': appendValue
    }
    result = sheet.append(spreadsheetId=spreadsheetId, range=range, valueInputOption='RAW', body=body).execute()
    return result


def new_row(service, spreadsheetId, sheetId, rows):
    sheet = service.spreadsheets()
    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheetId,
                        "dimension": "ROWS",
                        "startIndex": 1,
                        "endIndex": rows+1
                    },
                    "inheritFromBefore": "false"
                }
            }
        ]
    }
    result = sheet.batchUpdate(spreadsheetId=spreadsheetId, body=body).execute()
    return result
