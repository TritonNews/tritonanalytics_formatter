from downloader import download
from exporter import export_to
from sanitizer import sanitize

from pymongo import MongoClient

import logging
import os

def main():
  # Setup logging
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  divider = '-' * 30

  # Login to database
  logging.info(divider + " DATABASE LOGIN " + divider)
  db = MongoClient(os.environ.get("TRITON_ANALYTICS_MONGODB")).get_database("tritonanalytics")

  # Download recent analytics
  logging.info(divider + " DOWNLOAD PROCESS " + divider)
  download(os.environ.get("TRITON_ANALYTICS_FBID"), os.environ.get("TRITON_ANALYTICS_FBTOKEN"))

  # Export all stored analytics
  logging.info(divider + " EXPORT PROCESS " + divider)
  export_to(db)

  # Sanitize database
  logging.info(divider + " SANITIZATION " + divider)
  sanitize(db)

if __name__ == '__main__':
  main()