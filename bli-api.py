#/usr/bin/python

# yay, bli has an API!
	# /search/content/topic

# bottle app w/ 1 route for generating BLI Tables
	# queries are limited to >= 500 results returned
	# queries are sorted by totalShares
# it's async using gevent & greenlets

'''
Copyright 2012 Joshua S. Giardino, iAcquire

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''
	
try:
	import sys, logging, json, urllib

	import pymongo
	from pymongo import *

	from gevent import monkey; monkey.patch_all()
	from time import sleep
	from bottle import route, run, request
	
	import raven
	from raven import Client

	from raven.handlers.logging import SentryHandler
	from raven.conf import setup_logging
	
	logger = logging.getLogger()
	# If Sentry Fails, might as well fail the whole thing.
	client = Client('')
	handler = SentryHandler(client)
	setup_logging(handler)
except:
	print 'Fatal Error - Missing Dependency: ', sys.exc_info()[0]
	sys.exit()

try:
	conn = Connection('',27017,auto_start_request=False)
	db = conn.bli
	content = db[u'content'];
except Exception, ex:
	logging.exception('mongodb connection error')
	print 'Fatal Error - Missing Dependency: ', sys.exc_info()[0]
	sys.exit()



@route('/search/content/topic')
def searchBliByTopic():
	try:
		try:
			topic = request.query.get('topic')
		except Exception, ex:
			logging.exception('no name error: searchBliByTopic()')
			topic = None


		if topic is None:
			yield '{"response":{"error":"Sorry, the server received a malformed request. Please refresh the page and try your search again."}}'
		else:

			try:
				topic = urllib.unquote(topic)
				topic = unicode(topic.strip(),errors='ignore')
				topic = topic.lower()
			except Exception, ex:
				logging.exception('name to unicode error: searchBliByAuthorName()')
			
			try:
				resultList = content.find({u'topics':topic}).sort(u'totalShares', pymongo.DESCENDING).limit(100)
				resultCount = resultList.count()
				conn.end_request()
			
				if resultCount > 0:
					result = ''
					
					count = 0
					for record in resultList:
						url = record[u'url']
						urlNoHttp = url[7:]
						urlEncoded = urllib.quote_plus(url)
						urlNoHttp = urllib.quote_plus(urlNoHttp)
						shares = record[u'shares']
						httpCode = record[u'httpCode']
						result = result + url + ',' + httpCode + ','
						result = result + 'http://www.linkdiagnosis.com/?q=' + urlEncoded + ',http://wayback.archive.org/web/*/' + url + ',http://www.opensiteexplorer.org/links.html?no_redirect=1&page=1&site=' + url + ',http://www.majesticseo.com/reports/site-explorer?folder=&q=' + urlEncoded + '?IndexDataSource=H'
						count = count+1
						if count < resultCount:
							result = result + ';'
					
					result = result.rstrip(';')
					yield result
					
				else:
					yield '{"response":{"error":"Sorry,the Broken Link Index has no results for ' + topic + '. Our database is always growing, so check back soon!"}}'
			except Exception, ex:
				logger.exception('mongoDB Connection Error')
				yield '{"response":{"error":"Sorry, the server received a malformed request. Please refresh the page and try your search again."}}'
		
	except Exception, ex:
		logger.exception('unspecified api error')
		yield '{"response":{"error":"Sorry, the server received a malformed request. Please refresh the page and try your search again."}}'
		
logging.exception('Broken Link Index API Started')				
run(host='0.0.0.0', port=8080, server='gevent')
