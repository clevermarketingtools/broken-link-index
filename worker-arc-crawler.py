#!/usr/bin/python

# incorporates code from my personal library which is licensed under an Apache 2.0 License

'''
Copyright 2012 Joshua S. Giardino

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



# down and dirty failsafe to make sure we have all our dependencies
# if something is missing, it will terminate the worker and write the error to stdout.
# all errors after asyncCrawler's Init will just be written to Sentry instead of causing a termination.

try:
	import logging

	import raven
	from raven import Client

	from raven.handlers.logging import SentryHandler
	from raven.conf import setup_logging
	
	import tornado
	from tornado import httpclient
	from tornado import ioloop
	from tornado.stack_context import ExceptionStackContext 

	import lxml
	from pyquery import PyQuery as pq

	import pymongo
	from pymongo import *
	
	from hotqueue import HotQueue
	
	from urlparse import urlparse
	
	import nltk
	from nltk import *
	
	import sys, gzip, pickle, json
	from StringIO import StringIO

	from boto.s3.connection import S3Connection
	from boto.s3.key import Key	

	logger = logging.getLogger()
	# If Sentry Fails, might as well fail the whole thing.
	client = Client('')
	handler = SentryHandler(client)
	setup_logging(handler)

except:
	print 'Fatal Error - Missing Dependency: ', sys.exc_info()[0]
	sys.exit()

#configure appropriate mongodb servers and ports.
authoraServer = ''
authoraPort = 27017
	
bliServer = ''
bliPort = 27017

#configure HotQueue Server and port
arcQueueServer = ''
arcQueuePort = 6379

#aws keys
awsAccessKey = ''
awsSecretKey = ''

#setup stopwords because nltk is being difficult

stopwords = ['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am','is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will', 'just', 'don', 'should', 'now']

# async crawler based on tornado, expects a list of dicts derived from the ARC.

class asyncARCCrawler():
	def __init__(self):
		try:
			self.crawler = httpclient.AsyncHTTPClient()
			self.loop = ioloop.IOLoop.instance()
			self.crawled = 0
			self.total = 0
			self.authoraConn = Connection(authoraServer,authoraPort,auto_start_request=False)
			self.bliConn = Connection(bliServer,bliPort,auto_start_request=False)
			self.authoraDB = self.authoraConn.authora
			self.bliDB = self.bliConn.bli
			self.authorsCollection = self.authoraDB[u'authors']
			self.authorsContentCollection = self.authoraDB[u'content']
			self.bliContentCollection = self.bliDB[u'content']
			self.stopwords = stopwords
		except Exception, ex:
			logger.exception('Fatal Error Unable to initialize asyncCrawler')
			sys.exit()
			
	def append(self,siteURL):
		self.total = self.total + 1
		#print self.total
		#print 'queuing: ' + siteURL
		self.crawler.fetch(siteURL, self.handleRequest)

	def handleRequest(self,response):
		try:
			if response.error is not None:
				tmpResult = self.bliContentCollection.find({u'url':unicode(response.effective_url)})
				self.bliConn.end_request()
				if tmpResult.count() < 1:
					#print response.error
					code = unicode(str(response.code))
					for site in urlQueue:
						if site[u'url'] == response.effective_url:
							if site[u'source'] != None:
								uniDist = FreqDist()
								biDist = FreqDist()
								triDist = FreqDist()
							
								
								#removed based on data from the LXML Doc
								#source = unicode(site[u'source'],errors='ignore')
								source = lxml.html.fromstring(site[u'source'])
								text = source.text_content()
											
								wordbag = re.findall(r'\w+', text,flags = re.UNICODE | re.LOCALE)
								
								sorted = list()
								for word in wordbag:
									word = word.strip('()')
									word = word.lower()
									sorted.append(word)
								wordbag = []
								wordbag = sorted
								
								bi_grams = bigrams(wordbag)
								tri_grams = trigrams(wordbag)
								filtered_words = [w for w in wordbag if not w in self.stopwords]
								for gram in bi_grams:
									gram = gram[0] + " " + gram[1]
									gram = gram.strip()
									gram = unicode(gram)
									biDist.inc(gram)
								for gram in tri_grams:
									gram = gram[0] + " " + gram[1] + " " + gram[2]
									gram = gram.strip()
									gram = unicode(gram)
									triDist.inc(gram)
								for word in filtered_words:
									word = word.strip()
									gram = unicode(gram)
									uniDist.inc(word)
									
								topics = uniDist.keys()[:5] + biDist.keys()[:5] + triDist.keys()[:5]
								bliContentInsert = {u'url':unicode(response.effective_url),u'topics':topics,u'httpCode':code,u'shares':[{u'facebook':0},{u'twitter':0},{u'gplus':0}],u'totalShares':0,u'active':unicode('true')}
							else:
								bliContentInsert = {u'url':unicode(response.effective_url),u'topics':[unicode('test'),unicode('testing'),unicode('test this'),unicode('mongo test')],u'httpCode':code,u'shares':[{u'facebook':0},{u'twitter':0},{u'gplus':0}],u'totalShares':0,u'active':unicode('true')}
							
							#same idea as authors collection. Check often hoping for data integrity in async mode... no dupes! *fingers crossed*
							if self.bliContentCollection.find({u'url':unicode(response.effective_url)}).count() < 1:					
								bliContentID = self.bliContentCollection.insert(bliContentInsert,safe=True)
								broken.append(bliContentInsert)
								logger.exception('broken link index added a new piece of content: ' + response.effective_url)
								#queue job for bli social scores
								bliSocialCrawler.append('http://api.sharedcount.com/?url=' + response.effective_url)
								self.bliConn.end_request()
				self.crawled = self.crawled + 1
				self.stopMe()			
			else:
				#print response.effective_url
				# here to avoid unnecessary work, but repeated again later to ensure data integrity
				if self.authorsContentCollection.find({u'url':unicode(response.effective_url)}).count() < 1:
								
					if response.body != None:
						try:
							source = response.body
							#source = unicode(source,errors='ignore')
							source = lxml.html.fromstring(source)
							text = source.text_content()
							
							dom = pq(source)
							links = dom('a')
						
							authorBioURL = None
							for link in links:
								rel = pq(link).attr('rel')
								if rel != None:
									rel = rel.lower()
									if rel == 'author':
										#print 'rel=author detected'
										authorBioURL = pq(link).attr.href
										authorName = pq(link).text()
										authorName = unicode(authorName,errors='ignore')
										displayName = authorName
										authorName = authorName.replace("'","")
										authorNameSearch = list()
										authorNameSearch.append(authorName.lower())
										authorTmp = authorName.split(' ')
										for name in authorTmp:
											authorNameSearch.append(unicode(name.lower()))
											
							if authorBioURL != None and authorName != None and authorName != unicode(''):
								#generate topics
								uniDist = FreqDist()
								biDist = FreqDist()
								triDist = FreqDist()
								wordbag = re.findall(r'\w+', text,flags = re.UNICODE | re.LOCALE)
								
								sorted = list()
								for word in wordbag:
									word = word.strip('()')
									word = word.lower()
									sorted.append(word)
								wordbag = []
								wordbag = sorted
								
								bi_grams = bigrams(wordbag)
								tri_grams = trigrams(wordbag)
								filtered_words = [w for w in wordbag if not w in self.stopwords]
								for gram in bi_grams:
									gram = gram[0] + " " + gram[1]
									gram = gram.strip()
									gram = unicode(gram)
									biDist.inc(gram)
								for gram in tri_grams:
									gram = gram[0] + " " + gram[1] + " " + gram[2]
									gram = gram.strip()
									gram = unicode(gram)
									triDist.inc(gram)
								for word in filtered_words:
									word = word.strip()
									gram = unicode(gram)
									uniDist.inc(word)
								
								topics = uniDist.keys()[:5] + biDist.keys()[:5] + triDist.keys()[:5]
								
								# is authorBioURL in author collection?
								# if yes, return authorID for existing author
								# else insert a partial author record for the new author and return authorID
								#recordExists = self.authorsCollection.find({u'authorBioURL':unicode(authorBioURL)}).count()
								tmpResult =  self.authorsCollection.find({u'authorBioURL':unicode(authorBioURL)})
								self.authoraConn.end_request()
								if tmpResult.count() > 0:
									#reads are cheap
									authorData = self.authorsCollection.find_one({u'authorBioURL':unicode(authorBioURL)})
									self.authoraConn.end_request()
									if authorData is not None:
										authorID = authorData[u'_id']
										authorName = authorData[u'authorName']
								else:
									authorInsert = {u'authorName':unicode(displayName),u'authorBioURL':unicode(authorBioURL),u'accounts':[],u'authorSearch':authorNameSearch,u'active':unicode('true')}
									authorID = self.authorsCollection.insert(authorInsert,safe=True)
									self.authoraConn.end_request()
									authors.append(authorInsert)
									logger.exception('authora added a new author: ' + displayName)
								
								if authorID is not None and authorName is not None and authorName != unicode(''):
									# is content URL already in the collection?
									#if no, insert it
									#queue share lookup by contentURL
									#queue author lookup by authorBioURL
									#else: do nothing.
									
									
									#normally, I'd place this check after the initial response.body != None; but since we're async we have to check often
									if self.authorsContentCollection.find({u'url':unicode(response.effective_url)}).count() < 1:
										contentInsert = {u'authorID':unicode(authorID),u'authorName':unicode(authorName),u'url':unicode(response.effective_url),u'topics':topics,u'shares':[{u'facebook':0},{u'twitter':0},{u'gplus':0}],u'totalShares':0,u'sharesChecked':unicode('false'),u'authorSearch':authorNameSearch,u'active':unicode('true')}
										contentID = self.authorsContentCollection.insert(contentInsert,safe=True)
										self.authoraConn.end_request()
										logger.exception('authora added a new piece of content: ' + response.effective_url)
										
										#queue jobs for social metrics & author bio
										authorBioCrawler.append(authorBioURL)
										authoraSocialCrawler.append('http://api.sharedcount.com/?url=' + response.effective_url)
										
								self.crawled = self.crawled + 1
								self.stopMe()
							else:
								self.crawled = self.crawled + 1
								self.stopMe()
						except Exception, ex:
							logger.exception('Error Parsing URL: ' + response.effective_url)
							#print 'Error - Parsing URL ' + response.effective_url, sys.exc_info()[0]
							self.crawled = self.crawled + 1
							self.stopMe()
					else:
						print 'Empty Body'
						self.crawled = self.crawled + 1
						self.stopMe()
				else:
					self.crawled = self.crawled + 1
					self.stopMe()
		except Exception, ex:
			logger.exception('handle request error: asyncARCCrawler()')
			self.crawled = self.crawled + 1
			self.stopMe()
			
	def startCrawler(self):
		if self.total > 0:
			logger.exception(str(self.total))
			self.loop.start()
		else:
			self.loop.stop()

	def stopMe(self):
		if self.crawled == self.total:
			self.loop.stop()
			logger.exception('crawled: ' + str(self.crawled))
			logger.exception('total: ' + str(self.total))
			

		

# async crawler based on tornado, expects a link pointing to a relevant rel=me page.
# crawls site, extract "me's" and updates the author collection record associated with the authorBioURL
	
class asyncAuthorBioCrawler():
	def __init__(self):
		try:
			self.crawler = httpclient.AsyncHTTPClient()
			self.loop = ioloop.IOLoop.instance()
			self.crawled = 0
			self.total = 0
			self.authoraConn = Connection(authoraServer,authoraPort,auto_start_request=False)
			self.authoraDB = self.authoraConn.authora
			self.authorsCollection = self.authoraDB[u'authors']
			#self.authorsContentCollection = self.authoraDB[u'content']

		except Exception, ex:
			logger.exception('Fatal Error - Unable to initialize asyncAuthorBioCrawler')
			sys.exit()
		
	def append(self,siteURL):
		self.total = self.total + 1
		#print self.total
		#print 'queuing: ' + siteURL
		self.crawler.fetch(siteURL, self.handleRequest)
		
	def handleRequest(self,response):
		try:
			if response.error is None:
				if response.body != None:
					# extract links with rel=me
					# if not rel=me or no g+ rel=me, done.		
					try:
						source = lxml.html.fromstring(response.body)
						text = source.text_content()
						
						dom = pq(source)
						links = dom('a')
						
						authorServices = list()
						if self.authorsCollection.find({u'authorBioURL':unicode(response.effective_url)}).count() > 0:
							authorRecord = self.authorsCollection.find_one({u'authorBioURL':unicode(response.effective_url)})
							self.authoraConn.end_request()
							if authorRecord != None:
								authorID = authorRecord[u'_id']
							for link in links:
								rel = pq(link).attr('rel')
								if rel != None:
									rel = rel.lower()
									if rel == 'me':
										service = self.relMe2Social(pq(link).attr.href)
										if service != None:
											authorServices.append(service)
											#print service
							
							
							# ideally just append to list, but currently setting/overwriting - pushAll had a problem
							if authorID != None:
								#authorID = ObjectId(authorID)
								self.authorsCollection.update({'_id': authorID},{'$set' : {u'accounts': authorServices}},False,False)
								self.authoraConn.end_request()
								#print authorServices
							else:
								logger.exception('mongodb update not performed for author')
						self.crawled = self.crawled + 1
						self.stopMe()
					except Exception, ex:
						logger.exception('authorBioCrawler Error - Parsing RelMe on URL ' + response.effective_url)
						self.crawled = self.crawled + 1
						self.stopMe()
				else:
					logger.exception('authorBioCrawler Error - Empty Body Rel Me Loop on URL ' + response.effective_url)
					self.crawled = self.crawled + 1
					self.stopMe()
		except Exception, ex:
			logger.exception('handle request error: asyncAuthorBioCrawler()')
			self.crawled = self.crawled + 1
			self.stopMe()
			
	def startCrawler(self):
		if self.total > 0:
			self.loop.start()
		else:
			self.loop.stop()

	def stopMe(self):
		if self.crawled == self.total:
			logger.exception('author bios crawled: ' + str(self.crawled))
			logger.exception('author bios total: ' + str(self.total))
			self.loop.stop()
			
	# a necessary component of handleRelMe
	def relMe2Social(self, url):
		url_frags = urlparse(url)
		#print url_frags
		if url_frags[1] == 'plus.google.com':
			account = url_frags[2]
			#print account
			if account.find('/') > -1:
				account = account.split('/')
				if account[0] == '':
					account = account[1]
				else:
					account = account[0]
			else:
				account = None
			#print account
			if account != None:
				return {u'gplus': unicode(account)}
			else:
				return account
		elif url_frags[1] == 'twitter.com' or url_frags[1] == 'www.twitter.com':
			account = url_frags[2]
			#print account
			if account.find('intent') > -1:
				account = url_frags[4]
				equalsSign = account.find('=')
				account = account[equalsSign+1:]
			elif account.find('/') > -1:
				account = account.split('/')
				if account[0] == '':
					account = account[1]
				else:
					account = account[0]
			else:
				account = None
			#print account
			if account != None:
				return {u'twitter': unicode(account)}
			else:
				return account
		elif url_frags[1] == 'linkedin.com' or url_frags[1] == 'www.linkedin.com':
			account = url_frags[2]
			if account.find('/') > -1:
				account = account.split('/')
				#print account
				if account[0] == '':
					account = account[2]
				else:
					account = None
			else:
				account = None
			#print account
			if account != None:
				return {u'linkedin': unicode(account)}
			else:
				return account
		elif url_frags[1] == 'facebook.com' or url_frags[1] == 'www.facebook.com':
			account = url_frags[2]
			#print account
			if account.find('/') > -1:
				account = account.split('/')
				if account[0] == '':
					account = account[1]
				else:
					account = account[0]
			else:
				account = None
			#print account
			if account != None:
				return {u'facebook': unicode(account)}
			else:
				return account
		else:
			return None	


# crawlers for handling social updates to the BLI & Authora's Content Collections
# two tasks to update different collections/servers, but same general work

class asyncSocialCrawlerAuthora():
	def __init__(self):
		try:
			self.crawler = httpclient.AsyncHTTPClient()
			self.loop = ioloop.IOLoop.instance()
			self.crawled = 0
			self.total = 0
			self.authoraConn = Connection(authoraServer,authoraPort,auto_start_request=False)
			self.authoraDB = self.authoraConn.authora
			self.authorsContentCollection = self.authoraDB[u'content']
		except Exception, ex:
			logger.exception('Fatal Error - Unable to initialize asyncSocialCrawlerAuthora: ')
			sys.exit()
			
	def append(self,siteURL):
		self.total = self.total + 1
		self.crawler.fetch(siteURL, self.handleRequest)

	def handleRequest(self,response):
		try:
			if response.error is None and response.body != None:
				shareDict = json.loads(response.body)
				startURL = response.effective_url.split('=')
				startURL = startURL[1]
				#print startURL
				if self.authorsContentCollection.find({u'url':unicode(startURL)}).count() > 0:
					content = self.authorsContentCollection.find_one({u'url':unicode(startURL)})
					self.authoraConn.end_request()
					contentID = content[u'_id']
					try:
						contentFB = int(shareDict['Facebook']['share_count'])
					except:
						contentFB = 0
					contentTwitter = int(shareDict['Twitter'])
					contentGPlus = int(shareDict['GooglePlusOne'])
					contentTotal = contentFB + contentTwitter + contentGPlus
					#print contentID,contentFB
					#could increment, but we're going to set just in case of dupes.
					self.authorsContentCollection.update({'_id': contentID},{'$set' : {u'shares': [{u'facebook':contentFB},{u'twitter':contentTwitter},{u'gplus':contentGPlus}],u'totalShares':contentTotal}},False,False)
					self.authoraConn.end_request()
				self.crawled = self.crawled + 1
				self.stopMe()			

			else:
				logger.exception('Empty Body on asyncSocialCrawlerAuthora')
				self.crawled = self.crawled + 1
				self.stopMe()

		except Exception, ex:
			logger.exception('handle request error: asyncSocialCrawlerAuthora()')
			self.crawled = self.crawled + 1
			self.stopMe()
			
	def startCrawler(self):
		if self.total > 0:
			self.loop.start()
		else:
			self.loop.stop()

	def stopMe(self):
		if self.crawled == self.total:
			logger.exception('author social shares crawled: ' + str(self.crawled))
			logger.exception('author social shares total: ' + str(self.total))
			logger.exception('authorSocialCrawl complete')
			self.loop.stop()
			
			
			
class asyncSocialCrawlerBLI():
	def __init__(self):
		try:
			self.crawler = httpclient.AsyncHTTPClient()
			self.loop = ioloop.IOLoop.instance()
			self.crawled = 0
			self.total = 0
			self.bliConn = Connection(bliServer,bliPort,auto_start_request=False)
			self.bliDB = self.bliConn.bli
			self.bliContentCollection = self.bliDB[u'content']
		except Exception, ex:
			logger.exception('Fatal Error - Unable to initialize asyncSocialCrawlerBLI: ')
			sys.exit()
			
	def append(self,siteURL):
		self.total = self.total + 1
		self.crawler.fetch(siteURL, self.handleRequest)

	def handleRequest(self,response):
		try:
			if response.error is None and response.body != None:
				shareDict = json.loads(response.body)
				startURL = response.effective_url.split('=')
				startURL = startURL[1]
				#print startURL
				if self.bliContentCollection.find({u'url':unicode(startURL)}).count() > 0:
					content = self.bliContentCollection.find_one({u'url':unicode(startURL)})
					self.bliConn.end_request()
					contentID = content[u'_id']
					try:
						contentFB = int(shareDict['Facebook']['share_count'])
					except:
						contentFB = 0
						
					contentTwitter = int(shareDict['Twitter'])
					contentGPlus = int(shareDict['GooglePlusOne'])
					contentTotal = contentFB + contentTwitter + contentGPlus
					#print contentID,contentFB
					#could increment, but we're going to set just in case of dupes.
					self.bliContentCollection.update({'_id': contentID},{'$set' : {u'shares': [{u'facebook':contentFB},{u'twitter':contentTwitter},{u'gplus':contentGPlus}],u'totalShares':contentTotal}},False,False)
					self.bliConn.end_request()
				self.crawled = self.crawled + 1
				self.stopMe()			

			else:
				logger.exception('Empty Body on asyncSocialCrawlerBLI')
				self.crawled = self.crawled + 1
				self.stopMe()

		except Exception, ex:
			logger.exception('handle request error: asyncSocialCrawlerBLI()')
			self.crawled = self.crawled + 1
			self.stopMe()

	def startCrawler(self):
		if self.total > 0:
			self.loop.start()
		else:
			self.loop.stop()

	def stopMe(self):
		if self.crawled == self.total:
			logger.exception('bli social shares crawled: ' + str(self.crawled))
			logger.exception('bli social shares total: ' + str(self.total))
			self.loop.stop()



try:
	conn = S3Connection(awsAccessKey, awsSecretKey)
	buckets = conn.get_bucket('aws-publicdatasets')

except Exception, ex:
	logger.exception("Fatal Error - Failed to connect to S3")
	sys.exit()


arcQueue = HotQueue('arc', host=arcQueueServer, port=arcQueuePort, db=0)
arcKey = arcQueue.get()

# arcKey is received from HotQueue
#arcKey = '1262847559577_0.arc.gz'


logger.exception('crawl beginning')

#sentinel loop to continue processing queue till it's empty
while arcKey != None:
	logger.exception('fetching arc: ' + arcKey)
	

	
	# The S3 API Version streams the file in, decompresses the GZIP, and then converts it to a file object for iteration


	# stream an ARC from the Common Crawl Public Bucket
	# uses boto api

	key = buckets.get_key(arcKey)
	keyString = key.get_contents_as_string()
	keyString = gzip.GzipFile(fileobj=StringIO(keyString))
	
	logger.exception('finished fetching arc: ' + arcKey)
	urlQueue = list()
	count = 0
	flag = 'n'
	start = 'n'
	source = ''


	for line in keyString:
		if line.strip():
			line = line.lower()
			if line.find('content-type:') > -1:
				tmp = line.split(':')
				if tmp[1].find('text/html') > -1:
					flag = 'y'
				else: 
					flag = 'n'
			if flag == 'y':
				if line.find('x_commoncrawl_originalurl') > -1:
					url = line.split(':')
					protocol = url[1]
					url = url[2]
					url = protocol + ':' + url.strip()
					start = 'y'
					flag = 'n'
					
			if start == 'y':
				if line.find('date:') > -1:
					
					start = 'n'
					string = {u'url':unicode(url),u'source':source}
					
					#print 'source length: ', len(source)
					source = ''
					urlQueue.append(string)
					count = count+1
				else:
					source = source + line.strip()
					#print 'line appended'

	logger.exception('ARC Parsed. ' + str(count) + ' urls were found in ARC: ' + arcKey)


	authors = list()
	broken = list()
	arcCrawler = asyncARCCrawler()
	authorBioCrawler = asyncAuthorBioCrawler()
	authoraSocialCrawler = asyncSocialCrawlerAuthora()
	bliSocialCrawler = asyncSocialCrawlerBLI()

	for url in urlQueue:
		arcCrawler.append(url[u'url'])
		
	arcCrawler.startCrawler()
	#print 'control has returned'
	#print 'authors ', authors
	#print 'broken ', broken
	#print 'starting authorBioCrawler'
	authorBioCrawler.startCrawler()
	#print 'control has returned'
	authoraSocialCrawler.startCrawler()
	#print 'control has returned'
	bliSocialCrawler.startCrawler()
	logger.exception('crawl complete for ARC: ' + arcKey)
	arcKey = arcQueue.get()
#shut down the script once the queue is empty
logger.exception('crawl complete, worker exiting')
sys.exit()
