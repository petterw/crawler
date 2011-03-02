from celery.decorators import task
from celery.task.sets import subtask
from BeautifulSoup import BeautifulSoup
import urlparse
import urllib2

@task
def fetch_document(url, useragent, return_html = False):
	try:
		opener = urllib2.build_opener()
		opener.addheaders = [('User-agent', useragent)]
		response = opener.open(url)
		html = response.read()
		links = subtask(extract_urls).apply_async( [(url,html)] )
		# avoid filling memory with useless html if we don't want it
		if return_html:
			return (url, html, links, len(html))
		return (url, "", links, len(html))
	except:
		return (url,"", FakeAsyncResult(result = set()), 0)

@task
def fetch_robots_txt(url, useragent):
	try:
		opener = urllib2.build_opener()
		opener.addheaders = [('User-agent', useragent)]
		response = opener.open(url)
		robots_txt = response.read()
		return (url, robots_txt)
	except:
		return (url,"")

@task
def extract_urls(doc_tuple):
	urls = []
	try:
		soup = BeautifulSoup(doc_tuple[1])
		for tag in soup.findAll('a', href=True):
			urls.append(tag['href'])
		# return unique urls on page
		return set([(cleanup_url(urlparse.urljoin(doc_tuple[0], url)), doc_tuple[0]) for url in urls])
	except:
		# html too bad
		return set()

def cleanup_url(url):
	# quick fix for a problem urlparse has with utf-8
	return url.split("#".encode("utf-8"))[0].encode("utf-8")

class FakeAsyncResult:
	""" Limited placeholder for AsyncResult from celery, in case we don't want 
		to do anything, but the consumer of the result expects
		an AsyncResult with its usual methods
	"""
	def __init__(self, result = [], ready = True):
		self.result = result
		self.isready = ready
	def wait(self):
		return self.value
	def ready(self):
		return self.isready
