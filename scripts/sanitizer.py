"""
Sanitizes data in the MongoDB instance periodically: fills out missing columns, etc.
"""
from pymongo import MongoClient

import os

def sanitize(db):
  pass

def main():
  # Setup logging
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  # Login to database
  logging.info("Logging in to database ...")
  db_uri = os.environ.get("TRITON_ANALYTICS_MONGODB")
  db = MongoClient(db_uri).get_database("tritonanalytics")

  # Sanitize database every 15 minutes
  while True:
    sanitize(db)


if __name__ == '__main__':
  main()