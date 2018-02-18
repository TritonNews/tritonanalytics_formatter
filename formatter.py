from pymongo import MongoClient

import os
import logging
import glob
import pandas as pd

DATA_FOLDER = 'data'

def process(db):
  # Get a list of all data files
  logging.info("Reading CSV files ...")
  fb_page_files = glob.glob(os.path.join(DATA_FOLDER, "fb-page-*.csv"))
  fb_posts_files = glob.glob(os.path.join(DATA_FOLDER, "fb-posts-*.csv"))

  # Add all page data files to the database
  logging.info("Adding page analytics to database ...")
  for file in fb_page_files:
    process_fb_file(db.fbpages, file, 'Date')

  # Add all post data files to the database
  logging.info("Adding post analytics to database ...")
  for file in fb_posts_files:
    process_fb_file(db.fbposts, file, 'Post ID')

def process_fb_file(coll, file, row_id_column_header):
  row_id_doc_field = niceify_column_header(row_id_column_header)

  df = pd.read_csv(file)
  df = df.drop(df.index[[0, 1]]).fillna(0)

  for row_index, row in df.iterrows():
    doc = coll.find_one({ row_id_doc_field : row[row_id_column_header] })
    if doc:
      logging.info("Database already has record for row {0}='{1}'.".format(row_id_column_header, row[row_id_column_header]))
    else:
      logging.info("No record exists for row {0}='{1}'.".format(row_id_column_header, row[row_id_column_header]))

      # Build document to be inserted (preserve all columns under prettified field names)
      new_doc = {}
      for column in df:
        field_name = niceify_column_header(column)
        if '.' not in field_name: # Discard columns with banned symbols in their header
          new_doc[field_name] = row[column]

      logging.info("Inserting into database ...")
      coll.insert_one(new_doc)
      logging.info("Insertion succsesful.")

def niceify_column_header(column_header):
  return column_header.replace(' ', '_').replace('-', '_').lower()

def main():
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  logging.info("Logging in to database ...")
  db_uri = os.environ.get("TRITON_ANALYTICS_MONGODB")
  db = MongoClient(db_uri).get_database("tritonanalytics")
  logging.info("Login successful.")

  process(db)

if __name__ == '__main__':
  main()