"""
Exports statistics & data in csv/*.csv files to a MongoDB instance.
"""
from pymongo import MongoClient

import os
import logging
import glob
import pandas as pd
import argparse

def process(db):
  # Get a list of all data files
  logging.info("Reading CSV files ...")
  fb_page_files = glob.glob(os.path.join('csv', "fb-page-*.csv"))
  fb_posts_files = glob.glob(os.path.join('csv', "fb-posts-*.csv"))

  # Add all page data files to the database
  logging.info("Adding page analytics to database ...")
  for file in fb_page_files:
    process_fb_file(db.fbpages, file, 'Date')

  # Add all post data files to the database
  logging.info("Adding post analytics to database ...")
  for file in fb_posts_files:
    process_fb_file(db.fbposts, file, 'Post ID')

def process_fb_file(coll, file, unique_column_header):
  df = pd.read_csv(file)
  df = df.drop(df.index[[0, 1]]).fillna(0)

  for row_index, row in df.iterrows():
    doc = coll.find_one({ unique_column_header : row[unique_column_header] })
    if doc:
      logging.info("Database already has record for row {0}='{1}'.".format(unique_column_header, row[unique_column_header]))
    else:
      logging.info("No record exists for row {0}='{1}'.".format(unique_column_header, row[unique_column_header]))

      # Build document to be inserted (preserve all columns under prettified field names)
      new_doc = {}
      for column in df:
        if '.' not in column: # Discard columns with banned symbols in their header
          new_doc[column] = row[column]

      logging.info("Inserting into database ...")
      coll.insert_one(new_doc)
      logging.info("Insertion succsesful.")

def main():
  # Setup argument parser
  parser = argparse.ArgumentParser(description='Script that moves data from CSV files to MongoDB')
  parser.add_argument('db', help='URI of MongoDB used to hold analytics data')
  args = parser.parse_args()

  # Setup logging
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  # Login to database
  logging.info("Logging in to database ...")
  db = MongoClient(args.db).get_database("tritonanalytics")

  # Begin processing CSV files and exporting them
  process(db)

if __name__ == '__main__':
  main()