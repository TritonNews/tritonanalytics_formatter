"""
Downloads the last day's worth of data from Facebook.
"""
from pymongo import MongoClient

import os
import logging
import time
import requests

CSV_FOLDER = 'csv'
DOWNLOAD_TIMESPAN = 86400 # Given in seconds
DOWNLOAD_PING_DELAY = 30

FB_QUERY_URL = 'https://graph.facebook.com/v2.6/{0}/analytics_app_events_exports'
FB_CHECK_READY_URL = 'https://graph.facebook.com/v2.6/{0}?access_token={1}'

def download(fbid, fbtoken):
  timestamp = int(time.time())

  logging.info("Requesting analytics file from Facebook ...")
  res = requests.post(FB_QUERY_URL.format(fbid), data={
    access_token: fbtoken,
    start_ts: timestamp - DOWNLOAD_TIMESPAN
    end_ts: timestamp
  })
  logging.info("Server returned response {0} {1}.".format(res.status_code, res.reason))
  data = res.json()
  export_id = data.id
  logging.info("Job ID is {0}.".format(export_id))

  ready = False
  while not ready:
    logging.info("Checking download status ...")
    res = requests.get(FB_CHECK_READY_URL.format(export_id, fbtoken))
    logging.info("Server returned response {0} {1}.".format(res.status_code, res.reason))
    data = res.json()
    status = data.status
    ready = status == 'DONE'
    if not ready:
      sleep(DOWNLOAD_PING_DELAY)

def main():
  # Setup logging
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  # Perform the download
  logging.info("Downloading analytics ...")
  download(os.environ.get("TRITON_ANALYTICS_FBID"), os.environ.get("TRITON_ANALYTICS_FBTOKEN"))

if __name__ == '__main__':
  main()






