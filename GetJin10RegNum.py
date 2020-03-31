from GoogleConfig import GoogleConfig
import pandas as pd
import datetime

def main():
    analytics = GoogleConfig.get_ga()
    print("right")
    for i in range(0,30):
        startDate = '2019-11-01'+datetime.timedelta(days=i)
        endDate = '2019-11-01'+datetime.timedelta(days=i)
        response = analytics.reports().batchGet(
          body={
            'reportRequests': [
            {
              'viewId': '185946517',
              'dateRanges': [{'startDate': startDate, 'endDate': endDate}],
              'samplingLevel': 'LARGE',
              'metrics': [{'expression': 'ga:sessions', 'alias': 'sess'}],
              'dimensions': [{'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}],
              'orderBys': [{'fieldName': 'ga:sessions', 'sortOrder': 'DESCENDING'}]
            }]
          }
        ).execute()




if __name__ == '__main__':
    main()