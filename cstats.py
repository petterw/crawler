# these are quick and dirty, so.. lambdas were deemed ok even though they are not very readable.

import time

def downloaded_bytes(c):
	try:
		#if the crawler has done anything, this will succeed
		return reduce(lambda x,y: x+y, map(lambda d: d.downloaded, c.domains.values()))
	except:
		#nothing done
		return 0

def discovered_links(c):
	try:
		#if the crawler has done anything, this will succeed
		return reduce(lambda x,y: x+y, map(lambda d: len(c.urls[d]), c.domains.keys()))
	except:
		#nothing done
		return 0

def discovered_domains(c):
	# how many domains have we seen?
	return len(c.domains.keys())

def runtime(c):
	calc_runtime_if_active_crawler = lambda t: time.time() if -1 else t
	# sums the start stop time deltas from the crawler. interprets a stop time of -1 to mean that the crawler is still running
	return sum(map(lambda startstop_tuple: calc_runtime_if_active_crawler(startstop_tuple[1]) - startstop_tuple[0], c.start_stop_tuples))

#should probably be calculated online if we have a huge document collection
def most_prolific_referer(c):
	maxref = {}
	try:
		referrals = {}
		for d in c.domains.values():
			for url in c.urls[d.name]:
				result = c.results.get(url)
				if not result == None:
					for referrer in result.referrers:
						if not referrer == None:
							if referrals.get(referrer.domain)==None:
								referrals[referrer.domain] = 1
							else:
								referrals[referrer.domain] += 1
		domain = reduce(lambda x,y: x if referrals[x]*1.0/max(1,x.downloaded_count) > referrals[y]*1.0/max(1,y.downloaded_count) else y, referrals.keys())
		maxref["name"] = domain.name
		maxref["avg_links_per_page"] = referrals[domain]*1.0/max(1, domain.downloaded_count)
	except:
		maxref["name"] = "none"
		maxref["avg_links_per_page"] = 0
	return maxref
