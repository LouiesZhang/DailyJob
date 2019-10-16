import datetime

def getMass_report(analytics, viewId):
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': viewId,
          'dateRanges': [{'startDate': 'yesterday', 'endDate': 'yesterday'}],
          'samplingLevel': 'LARGE',
          'metrics': [{'expression': 'ga:sessions', 'alias': 'sess'}],
          'dimensions': [{'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}],
          'orderBys': [{'fieldName': 'ga:sessions', 'sortOrder': 'DESCENDING'}]
        }]
      }
  ).execute()

def get_user_message(analytics, viewId, rDate, GaId):
  realDate = datetime.datetime.strptime(rDate + '', '%Y-%m-%d').strftime('%Y-%m-%d')
  return analytics.reports().batchGet(
      body={
          'reportRequests': [
              {
                  'viewId': viewId,
                  'dateRanges': [{'startDate': realDate, 'endDate': realDate}],
                  'samplingLevel': 'LARGE',
                  'metrics': [{'expression': 'ga:sessions', 'alias': 'sessions'}],
                  'dimensions': [{'name': 'ga:dimension2'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}, {'name': 'ga:adContent'}, {'name': 'ga:keyword'}],
                  'dimensionFilterClauses': [
                      {
                          'filters': [
                              {
                                  'dimensionName': 'ga:dimension2',
                                  'expressions': [
                                      GaId+''
                                  ]
                              }
                          ]
                      }
                  ]
              }]
      }
  ).execute()

def get_user_message_by_account(analytics, viewId, rDate, account):
  realDate = datetime.datetime.strptime(rDate + '', '%Y-%m-%d').strftime('%Y-%m-%d')
  return analytics.reports().batchGet(
      body={
          'reportRequests': [
              {
                  'viewId': viewId,
                  'dateRanges': [{'startDate': realDate, 'endDate': realDate}],
                  'samplingLevel': 'LARGE',
                  'metrics': [{'expression': 'ga:sessions', 'alias': 'sessions'}],
                  'dimensions': [{'name': 'ga:dimension5'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}, {'name': 'ga:adContent'}, {'name': 'ga:keyword'}],
                  'dimensionFilterClauses': [
                      {
                          'filters': [
                              {
                                  'dimensionName': 'ga:dimension5',
                                  'expressions': [
                                      account+''
                                  ]
                              }
                          ]
                      }
                  ]
              }]
      }
  ).execute()

def get_user_message_igold(analytics, viewId, rDate, GaId):
  realDate = datetime.datetime.strptime(rDate + '', '%Y-%m-%d').strftime('%Y-%m-%d')
  if(realDate >= '2019-09-07'):
      url = 'member.igoldhk.com/opentrueaccount/RegistrationSuccess'
  else:
      url = 'nickName'
  return analytics.reports().batchGet(
      body={
          'reportRequests': [
              {
                  'viewId': viewId,
                  'dateRanges': [{'startDate': realDate, 'endDate': realDate}],
                  'samplingLevel': 'LARGE',
                  'metrics': [{'expression': 'ga:users', 'alias': 'users'}],
                  'dimensions': [{'name': 'ga:dimension2'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}, {'name': 'ga:adContent'}, {'name': 'ga:keyword'}, {'name': 'ga:searchDestinationPage'}],
                  'dimensionFilterClauses': [
                      {
                          'operator': 'AND',
                          'filters': [
                              {
                                  'dimensionName': 'ga:dimension2',
                                  'expressions': [
                                      GaId+''
                                  ]
                              },
                              {
                                  'dimensionName': 'ga:searchDestinationPage',
                                  'expressions': [
                                      url+''
                                  ]
                              }
                          ]
                      }
                  ]
              }]
      }
  ).execute()

def get_user_message_by_account_igold(analytics, viewId, rDate, account):
  realDate = datetime.datetime.strptime(rDate + '', '%Y-%m-%d').strftime('%Y-%m-%d')
  if (realDate >= '2019-09-07'):
      url = 'member.igoldhk.com/opentrueaccount/RegistrationSuccess'
  else:
      url = 'nickName'
  return analytics.reports().batchGet(
      body={
          'reportRequests': [
              {
                  'viewId': viewId,
                  'dateRanges': [{'startDate': realDate, 'endDate': realDate}],
                  'samplingLevel': 'LARGE',
                  'metrics': [{'expression': 'ga:users', 'alias': 'users'}],
                  'dimensions': [{'name': 'ga:dimension2'}, {'name': 'ga:sourceMedium'}, {'name': 'ga:campaign'}, {'name': 'ga:adContent'}, {'name': 'ga:keyword'}, {'name': 'ga:searchDestinationPage'}],
                  'dimensionFilterClauses': [
                      {
                          'operator': 'AND',
                          'filters': [
                              {
                                  'dimensionName': 'ga:searchDestinationPage',
                                  'expressions': [
                                      account+''
                                  ]
                              },
                              {
                                  'dimensionName': 'ga:searchDestinationPage',
                                  'expressions': [
                                      url+''
                                  ]
                              }
                          ]
                      }
                  ]
              }]
      }
  ).execute()