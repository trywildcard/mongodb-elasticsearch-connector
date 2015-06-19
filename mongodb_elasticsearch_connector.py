#!/usr/bin/env python

import argparse
from elasticsearch import Elasticsearch
import logging
import os
from pymongo import MongoClient


def sanitize_document(document, fields=[]):
  """
  Removes sensitive fields from a MongoDB document.

  Args:
    document: The MongoDB document to sanitize.
    fields: An array of fields to check against. Each field is treated as a substring
            of keys within the MongoDB document.

  Returns:
    A sanitized version of the document.
  """
  # document['mongo_id'] = document.pop('_id')
  for field in fields:
    [document.pop(key, None) for key in document.keys() if field in key]

  return document


def send_to_elasticsearch(document, index, doc_type='mongodb', client=None):
  """
  Sends the document to Elasticsearch.
  """

  logging.debug('Indexing document: %s', document)
  client.index(index=index, doc_type=doc_type, id=document.pop('_id'), body=document)


def process_collection(database, collection, index, blacklist=[], db_client=None, es_client=None):
  """
  Iterates over a collection in MongoDB. Each document in the collection is sanitized and sent
  to Elasticsearch.
  """

  collection_data = db_client[database][collection]
  for entry in collection_data.find():
    doc = sanitize_document(entry, blacklist)
    send_to_elasticsearch(doc, index, doc_type=collection.lower(), client=es_client)


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='Python script to export MongoDB collections to Elasticsearch.')
  parser.add_argument('-v', '--verbose', help='Increase output verbosity', action='store_true')
  parser.add_argument('-d', '--debug', help='Increase output verbosity for debugging', action='store_true')
  parser.add_argument('--mongo_host', default=os.environ.get('MONGO_HOST', 'localhost'),
                      help='Address of the MongoDB host.')
  parser.add_argument('--mongo_port', default=os.environ.get('MONGO_PORT', 27017), type=int,
                      help='Port to connect to MongoDB on.')
  parser.add_argument('--database', default=os.environ.get('MONGO_DATABASE'),
                      help='MongoDB database to use.')
  parser.add_argument('--collection', default=os.environ.get('MONGO_COLLECTION'),
                      help='MongoDB collection to export.')
  parser.add_argument('--elasticsearch_host', default=os.environ.get('ES_HOST', 'localhost'),
                      help='Address of the Elasticsearch host.')
  parser.add_argument('--elasticsearch_port', default=os.environ.get('ES_PORT', 9200), type=int,
                      help='Port to connect to Elasticsearch on.')
  parser.add_argument('--index', default=os.environ.get('ES_INDEX'),
                      help='Index in Elasticsearch to store the collection\'s data.')
  fields = os.environ.get('MONGO_BLACKLIST', [])
  if fields:
    fields = fields.split(',')
  parser.add_argument('--blacklist', default=fields, nargs="+",
                      help='Fields to sanitize out of the MongoDB entries.')
  args = parser.parse_args()

  if args.debug:
    level = logging.DEBUG
  elif args.verbose:
    level = logging.INFO
  else:
    level = logging.WARN
  logging.basicConfig(format='%(asctime)s - %(name)s:%(levelname)s - %(message)s', level=level)

  index = args.index or 'mongodb-{}-{}'.format(args.database, args.collection)

  logging.warn('Starting export of %s.%s to %s', args.database, args.collection, index)

  index = index.lower()
  es_client = Elasticsearch([{'host': args.elasticsearch_host, 'port': args.elasticsearch_port}])
  # ignore 400 cause by IndexAlreadyExistsException when creating an index
  es_client.indices.create(index=index, ignore=400)

  db_client = MongoClient(args.mongo_host, args.mongo_port)

  process_collection(args.database, args.collection, index,
                     blacklist=args.blacklist,
                     db_client=db_client, es_client=es_client)

  db_client.close()

  logging.warn('Finished export')
