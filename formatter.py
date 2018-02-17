from pymongo import MongoClient

import os
import logging

def process(db):
  pass

def main():
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  logging.info("Accessing database ...")
  db_uri = os.environ.get("TRITON_ANALYTICS_MONGODB")
  db = MongoClient(db_uri).get_database()
  logging.info("Login successful")

  process(db)

if __name__ == '__main__':
  main()