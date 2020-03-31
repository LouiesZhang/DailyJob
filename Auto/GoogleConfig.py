import httplib2
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


GA_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
SHEET_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY_FILE_LOCATION = './gaforpython-f28f94d19c65.json'
def get_ga():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, GA_SCOPES)
  hp = credentials.authorize(httplib2.Http(proxy_info=httplib2.ProxyInfo(httplib2.socks.PROXY_TYPE_HTTP_NO_TUNNEL, 'localhost', 1080)))
  http = credentials.authorize(hp)
  analytics = build('analyticsreporting', 'v4', http=http, cache_discovery=False)
  return analytics

def get_google_sheet():
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
    KEY_FILE_LOCATION, SHEET_SCOPES)
  hp = credentials.authorize(
    httplib2.Http(proxy_info=httplib2.ProxyInfo(httplib2.socks.PROXY_TYPE_HTTP_NO_TUNNEL, 'localhost', 1080)))
  http = credentials.authorize(hp)
  service = build('sheets', 'v4', http=http, cache_discovery=False)
  return service