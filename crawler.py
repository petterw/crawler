import fetcher, time, urlparse, robotexclusionrulesparser, pickle
from datetime import datetime, timedelta

class Crawler:
	""" The main crawler class. Instantiate this with a seed and tell it
		to start crawling with .crawl()
	"""
	def __init__(self,
		seed, # The initial list of urls to visit
		robots_txt_name = "crawler", # which useragent to obey robots.txt rules for
		useragent_string = "crawler", # useragent string to send when crawling
		default_crawl_delay = 20, # the minimum time between fetches from a domain
		obey_robots_txt = True, # Be nice?
		schemes = ["http"], # link types to follow
		crawl_domains = [], # optionally, restrict the crawler to these domains
		concurrent_fetchers = 1): # The number of documents that may be scheduled for
		# fetching at the same time. Don't set this above the number of celery worker
		# processes.

		#config import
		self.seed = seed
		self.robots_txt_name = robots_txt_name
		self.useragent_string = useragent_string
		self.obey_robots_txt = obey_robots_txt
		self.default_crawl_delay = default_crawl_delay
		self.schemes = schemes
		self.crawl_domains = crawl_domains
		self.concurrent_fetchers = concurrent_fetchers

		#setup
		self.wait_time = 0.1
		self.urls = {}
		self.links = {}
		self.domains = {}
		self.url_extraction_queue = []
		self.retrieval_queue = []
		self.retrieval_in_progress = []
		self.results = {None:None}
		self.done = False
		self.started = False
		self.start_stop_tuples = [(time.time(), -1)]

		# to avoid having a website starve the crawling process with excessive crawl delays:
		self.too_far_ahead_to_schedule = timedelta(seconds=self.wait_time)

	def add_url(self, url_tuple):
		""" add a url to the retrieval queue, without starting to download it
			This creates a Domain instance if the domain hasn't been seen before,
			to keep track of crawl intervals, robot exclusion etc. It also
			updates some internal data so that statistics may be calculated,
			and so that a more accurate rank score can be given to other urls.
		"""
		#avoid parsing more than once
		parts = urlparse.urlparse(url_tuple[0])
		#if it's a url we want to visit
		if parts[0] in self.schemes and (len(self.crawl_domains) == 0 or (parts[1] in self.crawl_domains)):
			# extract domain
			dname = parts[1]

			#if we've never seen this domain
			if self.domains.get(dname) == None:
				# create a Domain instance
				self.domains[dname] = Domain(name = dname, crawler = self)
				# and an array to keep track of urls for this domain
				self.urls[dname] = []

			# if this url is new to us
			if self.results.get(url_tuple[0]) == None:
				#add to urls for domain
				self.urls[dname].append(url_tuple[0])
				# create a Document instance for url
				document =  Document(url_tuple[0],
									 self.results[url_tuple[1]],
									 self.domains[dname])
				# add url to list of candidates for retrieval
				self.retrieval_queue.append(document)
				# keep track of Document
				self.results[url_tuple[0]] = document

			else:
				#if we have seen this url before, we just track the extra incoming link
				self.results.get(url_tuple[0]).add_referrer(self.results[url_tuple[1]])

	def crawl(self, save_frequency = None, termination_checker = lambda c: False):
		""" Start crawl. Default termination checker says to never stop.
			Plug your own in that says otherwise if this is wanted.
		"""
		# Add seed urls to queue if we're not resuming a suspended crawl
		if not self.started:
			for s in self.seed:
				self.add_url((s, None))
			#prevent resuming of crawl from re-adding seeds
			self.started = True

		# To allow resuming even after we were done the first time:
		self.done = False

		# Setup tracking of last periodic save time
		if save_frequency != None:
			last_save = datetime.now() - save_frequency

		# start of crawling:
		# yes we are managing the crawling process with polling.
		# celery would do so internally anyway, and we want more control
		while not self.done:
			# make a best effort attempt to save the state periodically
			if save_frequency != None and save_frequency + last_save < datetime.now():
				print "Periodic save made to " + self.suspend()
				last_save = datetime.now()
			# Bool which is checked to avoid sorting the potentially large
			# retrieval queue if possible
			new_links = False

			""" Consider moving crawled documents from in progress queue to
				url extraction queue, or remove them from crawling altogether
				if we were blocked by robots.txt
			"""
			for doc in self.retrieval_in_progress:
				if doc.blocked:
					# robots.txt block => remove so we don't starve the crawl process
					self.retrieval_in_progress.remove(doc)
				if doc.task.ready() and doc.task.result[2].ready():
					self.retrieval_in_progress.remove(doc)
					self.url_extraction_queue.append(doc)

			""" Consider adding urls from result to crawling queue """
			self.url_extraction_queue.sort(key=self.rank)
			for doc in self.url_extraction_queue:
				self.url_extraction_queue.remove(doc)
				if len(doc.get_contents()[2].result) > 0:
					new_links = True
				for url_tuple in doc.get_contents()[2].result:
					if self.links.get(doc.url)==None:
						self.links[doc.url] = [url_tuple[0]]
					else:
						self.links[doc.url] = self.links[doc.url] + [url_tuple[0]]
					self.add_url(url_tuple)

			""" Consider crawling some new urls from queue: """
			if self.concurrent_fetchers - len(self.retrieval_in_progress) > 0:
				if new_links:
					self.retrieval_queue.sort(key=self.rank)
				for doc in self.retrieval_queue:
					self.retrieval_queue.remove(doc)
					doc.retrieve()
					self.retrieval_in_progress.append(doc)
					if self.concurrent_fetchers - len(self.retrieval_in_progress) == 0:
						break

			# avoid checking progress too often
			time.sleep(self.wait_time)

			# Are we done, or out of work? (no links to follow, waiting for 0 results)
			if termination_checker(self) or (len(self.retrieval_queue) == 0 and len(self.retrieval_in_progress) == 0 and len(self.url_extraction_queue) == 0):
				self.done = True

	def suspend(self):
		""" Suspends crawl to file and returns filename """
		# store stop time
		self.start_stop_tuples.append( (self.start_stop_tuples.pop()[0], time.time()) )
		filename = str(int(time.time()))+".suspended_crawl"
		f = open(filename,"w")
		pickle.dump(self, f)
		f.close()
		return filename

	def rank(self, document):
		""" Assigns score to a document, used for sorting retrieval queue to find next
			urls to crawl. This is probably not the best ranking method but hey.. WIP^TM
		"""
		# if nobody thinks the site is worth linking to, then who are we to argue?
		if len(document.referrers) == 0:
			return 0.0

		# if we're not allowed to crawl the site before the next crawl management pass,
		# assign a low score to avoid starving the crawling process with waiting workers
		if not document.domain.robots_txt_task.ready() or document.domain.too_long_until_crawl(too_long = self.too_far_ahead_to_schedule):
			return 0.0

		# assign a score based on who links to the document
		ancestor_linkjuice = 0.0
		for referrer in document.referrers:
			if referrer != None and self.links[referrer.url] != None and not document.domain == referrer.domain:
				ancestor_linkjuice += 1.0/(len(self.links[referrer.url]))

		return ancestor_linkjuice/len(self.results)

class Document:
	""" Class representing a document (url + incoming link info + crawl
		state + contents)
	"""
	def __init__(self, url, referrer, domain):
		self.url = url
		self.referrers = [referrer]
		self.domain = domain
		self.crawl_time = None
		self.blocked = False
		self.task = fetcher.FakeAsyncResult(ready=False)

	def retrieve(self):
		""" Start retrieval of document.

			Treats crawl-delay as a *guideline*, as tasks scheduled for a specific
			time may be executed later. But on average, it will be obeyed.

			This is a limitation of the crawler, and should be fixed.

			To really obey crawl-delay, a safety margin should be added, and made into
			a task expiration time.
		"""

		# check if we're allowed to crawl
		if self.domain.allows_crawling(self.url):
			# farm out crawl job
			self.crawl_time = self.domain.claim_next_crawl_time()
			self.task = fetcher.fetch_document.apply_async(args = [ self.url, self.domain.crawler.useragent_string],
														   eta = self.crawl_time)

		else:
			# Blocked by robots.txt
			self.blocked = True


	def __eq__(self, other):
		""" Comparison override to be able to use class sensibly in queues """
		if other == None:
			return False
		return self.url == other.url

	def __hash__(self):
		""" Hash override to be able to use class in dicts """
		return hash(self.url)

	def get_contents(self):
		""" Blocking method to wait for and get the retrieval result """
		try:
			self.contents
		except:
			print "<<< " + self.url
			self.contents = self.task.wait()
			self.domain.downloaded += self.contents[3]
			self.domain.downloaded_count += 1
		return self.contents

	def add_referrer(self, referrer):
		""" gain an incoming link """
		self.referrers.append(referrer)

class Domain:
	""" A class representing a single domain, so we can keep track of
		allowable crawltimes, robot exclusion etc
	"""
	def __init__(self, name, crawler):
		# config
		self.name = name
		self.crawler = crawler
		# counters for statistics
		self.downloaded = 0
		self.downloaded_count = 0
		# robots.txt handling
		self.crawl_delay = timedelta(seconds=crawler.default_crawl_delay)
		self.last_crawl_time = datetime.now() - self.crawl_delay
		self.rp = robotexclusionrulesparser.RobotExclusionRulesParser()
		self.robots_txt_task = fetcher.FakeAsyncResult(ready=False)
		self.parsed_robots_txt = False
		self.setup_robots_txt()

	def setup_robots_txt(self):
		""" Download and parse robots.txt if we care about it """
		if self.crawler.obey_robots_txt:
			self.parsed_robots_txt = False
			try:
				# This should be made async later
				self.robots_txt_task = fetcher.fetch_robots_txt.apply_async(
					args = ['http://'+self.name+'/robots.txt', self.crawler.useragent_string])
				#self.rp.fetch()
			except:
				# if we couldn't get robots.txt, that's just too bad.. :p
				pass

	def parse_robots_txt(self):
		""" take our async result and parse it (blocking) """
		self.rp.parse(self.robots_txt_task.wait()[1])
		if self.rp.get_crawl_delay(self.crawler.robots_txt_name) != None:
				self.crawl_delay = max(timedelta(seconds = self.rp.get_crawl_delay(self.crawler.robots_txt_name)), self.crawl_delay)
		self.parsed_robots_txt = True

	def __eq__(self,other):
		""" Override eq so we can do better than object id comparisons """
		if other == None:
			return False
		return self.name == other.name

	def __hash__(self):
		""" Override has so we can use Domain as key in dicts etc """
		return hash(self.name)

	def claim_next_crawl_time(self):
		""" Claim a crawl time """
		if self.rp.is_expired():
			self.setup_robots_txt()
		if not self.parsed_robots_txt:
			self.parse_robots_txt()
		self.last_crawl_time = self.last_crawl_time + self.crawl_delay
		return self.last_crawl_time

	def defer_crawl(self):
		""" Undo claim of crawl time """
		self.last_crawl_time = self.last_crawl_time - self.crawl_delay

	def allows_crawling(self, url):
		""" Can we crawl this url? """
		if not self.crawler.obey_robots_txt:
			return True
		# if robots.txt is expired, refresh it
		if self.rp.is_expired():
			self.setup_robots_txt()
		# some more thought needs to go into how to avoid blocking on robots.txt
		if not self.parsed_robots_txt:
			self.parse_robots_txt()
		return self.rp.is_allowed(self.crawler.robots_txt_name, urlparse.urlparse(url)[2])

	def too_long_until_crawl(self, too_long):
		""" Avoid starvation of crawling process """
		return self.last_crawl_time + self.crawl_delay > datetime.now() + too_long


def resume(suspended_crawl):
	""" Reads suspended crawl from file and returns crawler object. Restart
		crawling with .crawl()

		Remember to handle file exceptions..
	"""
	crawler = pickle.load(open(suspended_crawl))

	# store start time
	crawler.start_stop_tuples.append( (time.time(), -1) )
	return crawler

""" Our very own exception """
class CrawlingError(Exception):
	def __init__(self,msg):
		self.msg = msg
