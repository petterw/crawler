"""
An example invocation of the crawler
"""

import sys, getopt, time
from datetime import timedelta
from crawler import Crawler, resume
import cstats

def crawl(c = None):
	if c == None:
		c = Crawler(
			seed = [], # your seed urls here 
			default_crawl_delay = 20, 
			obey_robots_txt = True,
			document_fetchers = 15,
			robots_txt_fetchers = 5) #start at least this many celery workers
	
	try:
		# start crawling, with this tasks specific termination criteria and 
		# a save period of 20 seconds
		c.crawl(
			termination_checker = example_task_termination_checker,
			save_frequency = timedelta(seconds = 20))
		
	finally:
		
		# if we were killed or finished, suspend crawl state to file.
		# revive the crawl with resume from crawler.py to explore results
		print "\n\n\nSuspended crawl to " + c.suspend()
		
		# print some statistics
		print "Downloaded bytes: " + str(cstats.downloaded_bytes(c))
		print "Discovered links: " + str(cstats.discovered_links(c))
		print "Discovered domains: " + str(cstats.discovered_domains(c))
		print "Runtime: " + str(cstats.runtime(c)) + " seconds"
		maxref = cstats.most_prolific_referer(c)
		print "Most prolific referrer was " + maxref["name"] + " with an average of " + str(maxref["avg_links_per_page"]) + " outgoing links per page."+"\n"

def example_task_termination_checker(crawler):
	""" Checks for the specific termination critera for this task 
		Note that it is only checked once per pass of the
		crawl management loop, so exceeding the termination critera
		by some small number of items discovered is expected.
	"""
	return cstats.discovered_links(crawler) >= 10000 or cstats.discovered_domains(crawler) >= 100

def main(argv=None):
	if argv is None:
		argv = sys.argv
	if len(argv) == 3 and argv[1] == "--resume" and argv[2].find("suspended_crawl") > -1:
		print "Resuming crawl from " + argv[2]
		c = resume(argv[2])
		crawl(c)
	else:
		print "Starting new crawl"
		crawl()
	

if __name__ == "__main__":
    sys.exit(main())
