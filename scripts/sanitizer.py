"""
Sanitizes data in the MongoDB instance periodically: fills out missing columns, etc.
Meant to run periodically as a cron job on a server
"""
from pymongo import MongoClient

from collections import defaultdict

import os
import time
import logging

def sanitize(db):
  sanitize_collection(db.fbpages)
  sanitize_collection(db.fbposts)

def sanitize_collection(coll):
  # Count column appearances & number of documents in the collection
  num_docs = 0
  column_appearances = defaultdict(int)
  column_appearances_only_zeroes = defaultdict(int)

  for doc in coll.find():
    for column, value in doc.items():
      column_appearances[column] += 1
      if value == 0 or value == '0':
        column_appearances_only_zeroes[column] += 1
    num_docs += 1

  # Every value corresponding to a field is trivial (equals 0)
  trivial_columns = \
    {column for column, appearances in column_appearances.items() if appearances == column_appearances_only_zeroes[column]} # Remove from all documents

  # A field that doesn't necessarily appear in every document but there are nontrivial values associated with it
  incomplete_essential_columns = \
    {column for column, appearances in column_appearances.items() if appearances > column_appearances_only_zeroes[column] and appearances < num_docs} # Expand out to every document

  # Modify each affected document, update if modifications were made
  for doc in coll.find():
    original_lines = len(doc)
    modifications_made = False
    for column in incomplete_essential_columns:
      if column not in doc:
        doc[column] = 0
        modifications_made = True
    for column in trivial_columns:
      if column in doc:
        del doc[column]
        modifications_made = True
    if modifications_made:
      logging.info("Modifying document {0} ...".format(doc['_id']))
      logging.info("{0} original lines, now {1} lines.".format(original_lines, len(doc)))
      coll.replace_one({ "_id": doc['_id'] }, doc, upsert=False)
    else:
      logging.info("No modifications made to document {0}.".format(doc['_id']))

def main():
  # Setup logging
  logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',level=logging.INFO)

  # Login to database
  logging.info("Logging in to database ...")
  db_uri = os.environ.get("TRITON_ANALYTICS_MONGODB")
  db = MongoClient(db_uri).get_database("tritonanalytics")

  # Sanitize database
  logging.info("Sanitizing database ...")
  start_time = time.time()
  sanitize(db)
  logging.info("Sanitization took {0:.2f} seconds.".format(time.time() - start_time))


if __name__ == '__main__':
  main()