from GoogleConfig import GoogleConfig
from GoogleDef import GaGet
from Util import TranUtil


def main():
  analytics = GoogleConfig.get_ga()
  response = GaGet.getMass_report(analytics, '196624990')
  TranUtil.insert_massResponse(response)

if __name__ == '__main__':
  main()