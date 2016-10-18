#!/usr/bin/python
#


import logging
from cgi import parse_qs
from datetime import datetime
import re
import random
import os
import string
import urllib
from urlparse import urlparse
import json
import webapp2
from google.cloud import bigquery
from google.appengine.api import taskqueue



# _ENCODE_TRANS_TABLE = string.maketrans('-: .@', '_____')

class BaseHandler(webapp2.RequestHandler):
    """The other handlers inherit from this class.  Provides some helper methods
    for rendering a template."""

    @webapp2.cached_property
    def jinja2(self):
      return jinja2.get_jinja2(app=self.app)

    def render_template(self, filename, template_args):
      self.response.write(self.jinja2.render_template(filename, **template_args))



class RunQueryQueueHandler(BaseHandler):

    def post(self):
        logging.info('running task queue item')

class CronQueryHandler(BaseHandler):

    def get(self):
        logging.info('running cron query')
        # Add the task to the default queue.
        taskqueue.add(queue_name='runquery-queue', url='/runquery_queue', params={'trigger': 'cron'})


class MainHandler(BaseHandler):
    """Handles search requests for comments."""



    def get(self):
        """Handles a get request with a query."""
        #client = datastore.Client(project = 'spartan-tesla-91409', namespace='Vouchers')
        #res = client.query(kind='Voucher_Batches').fetch()
        #logging.info(list(res))

        #gcs_client = storage.Client(project = 'spartan-tesla-91409')
        #for bucket in gcs_client.list_buckets():
        #    logging.info(bucket)

        bq_client = bigquery.Client(project = 'spartan-tesla-91409')
        query = bq_client.run_sync_query('SELECT * FROM [spartan-tesla-91409:CDW_DM.COMMUNITY] LIMIT 1000')
        query.timeout_ms = 3 * 1000
        logging.info('just going to start query')
        query.run()             # API request
        logging.info('finished query')

        logging.info(query.complete)
        logging.info(len(query.rows))
        
        #get field names
        for field in query.schema:
            logging.info(field.name)
        #get result data
        rows = query.rows
        token = query.page_token

        while True:
            logging.info(rows)
            for row in rows:
                logging.info(row)
            if token is None:
                break
            rows, total_count, token = query.fetch_data(
                page_token=token)       # API request
        

        self.response.write('Main')

   
logging.getLogger().setLevel(logging.DEBUG)


application = webapp2.WSGIApplication([
    ('/cron_query', CronQueryHandler),
    ('/runquery_queue', RunQueryQueueHandler),
    ('/.*', MainHandler)

    ],
    debug=True)


