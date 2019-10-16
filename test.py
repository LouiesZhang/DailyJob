from GoogleConfig import GoogleConfig

analytics = GoogleConfig.get_ga()
print("yes")
res = analytics.reports().batchGet(
      body={
          'reportRequests': [
              {
                  'viewId': '185946517',
                  'dateRanges': [{'startDate': '2019-10-09', 'endDate': '2019-10-09'}],
                  'samplingLevel': 'LARGE',
                  'metrics': [{'expression': 'ga:users', 'alias': 'users'}],
                  'dimensions': [{'name': 'ga:dimension2'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}, {'name': 'ga:adContent'}, {'name': 'ga:keyword'}, {'name': 'ga:searchDestinationPage'}]
              }]
      }
  ).execute()
print(res)