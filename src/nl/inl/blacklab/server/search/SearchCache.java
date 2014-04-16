package nl.inl.blacklab.server.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class SearchCache {
	private static final Logger logger = Logger.getLogger(SearchCache.class);

	/** Max time searches are allowed to run (5 minutes) */
	public static  int MAX_SEARCH_TIME_SEC = 5 * 60;

	/** The cached search objects. */
	Map<SearchParameters, Job> cachedSearches;

	/** Maximum size in bytes to target, or -1 for no limit. NOT IMPLEMENTED YET. */
	long maxSizeBytes = -1;

	/** Maximum number of searches to cache, or -1 for no limit. Defaults to (a fairly low) 20.*/
	int maxNumberOfSearches = 20;

	/** Maximum age of a cached search in seconds. May be exceeded because it is only cleaned up when
	 *  adding new searches. Defaults to one hour. */
	int maxSearchAgeSec = 3600;

	/** (Estimated) size of the cache. Only updated in removeOldSearches, so may not
	 * always be accurate. */
	private long cacheSizeBytes;

	/**
	 * Initialize the cache.
	 *
	 */
	public SearchCache() {
		cachedSearches = new HashMap<SearchParameters, Job>();
		//logger.debug("Cache created.");
	}

	/**
	 * Get a search from the cache if present.
	 *
	 * @param searchParameters the search parameters
	 * @return the Search if found, or null if not
	 */
	public Job get(SearchParameters searchParameters) {
		Job search = cachedSearches.get(searchParameters);
		if (search == null) {
			//logger.debug("Cache miss: " + searchParameters);
		} else {
			//logger.debug("Cache hit: " + searchParameters);
			search.resetLastAccessed();
		}
		return search;
	}

	/** Put a search in the cache.
	 *
	 * Also cleans older searches from the cache if necessary.
	 *
	 * @param search the search object
	 */
	public void put(Job search) {
		// Search already in cache?
		SearchParameters searchParameters = search.getParameters();
		if (cachedSearches.containsKey(searchParameters)) {
			if (cachedSearches.get(searchParameters) != search) {
				throw new RuntimeException("Cache already contains different search object!");
			}
			// Same object already in cache, do nothing
			logger.debug("Same object put in cache twice: " + searchParameters);
			return;
		}

		// Put search in cache
		//logger.debug("Put in cache: " + searchParameters);
		cachedSearches.put(searchParameters, search);
		removeOldSearches();
	}

	/**
	 * Get rid of all the cached Searches.
	 */
	public void clearCache() {
		cachedSearches.clear();
		logger.debug("Cache cleared.");
	}

	/**
	 * If the cache exceeds the given parameters, clean it up by
	 * removing less recently used searches.
	 */
	private void removeOldSearches() {

		// OPT: could be optimized a little bit

		// Sort cache by last access time
		List<Job> lastAccessOrder = new ArrayList<Job>(cachedSearches.values());
		Collections.sort(lastAccessOrder); // put stalest first

		calculateSizeBytes(lastAccessOrder);

		// Get rid of old searches
		boolean lookAtCacheSizeAndSearchAccessTime = true;
		for (Job search: lastAccessOrder) {
			if (!search.finished() && search.executionTimeMillis() / 1000 > MAX_SEARCH_TIME_SEC) {
				// Search is taking too long. Cancel it.
				logger.debug("Search is taking too long, cancelling: " + search);
				search.cancelJob();

				// For now, remove from cache, but we should really blacklist these
				// kinds of searches so repeating them doesn't matter.
				// TODO blacklist
				cachedSearches.remove(search.getParameters());

			} else if (lookAtCacheSizeAndSearchAccessTime && cacheTooBig() || searchTooOld(search)) {
				// Search is too old or cache is too big. Keep removing searches until that's no longer the case
				//logger.debug("Remove from cache: " + search);
				cachedSearches.remove(search.getParameters());
			} else {
				// Cache is no longer too big and these searches are not too old. Stop checking that,
				// just check for long-running searches
				lookAtCacheSizeAndSearchAccessTime = false;
			}
		}
	}

	private long calculateSizeBytes(Collection<Job> collection) {
		// Estimate the total cache size
		cacheSizeBytes = 0;
		for (Job search: collection) {
			cacheSizeBytes += search.estimateSizeBytes();
		}
		return cacheSizeBytes;
	}

	/**
	 * Checks if the cache size in bytes or number of searches is too big.
	 *
	 * Only applies if maxSizeBytes >= 0 or maxSizeSearcher >= 0.
	 *
	 * @return true iff the cache is too big.
	 */
	private boolean cacheTooBig() {
		boolean tooManySearches = maxNumberOfSearches >= 0
				&& cachedSearches.size() > maxNumberOfSearches;
		/*if (tooManySearches)
			logger.debug("Cache has too many searches (" + cachedSearches.size() + " > "
					+ maxNumberOfSearches + ").");*/
		boolean tooMuchMemory = maxSizeBytes >= 0 && cacheSizeBytes > maxSizeBytes;
		/*if (tooMuchMemory)
			logger.debug("Cache takes too much memory (" + cacheSizeBytes + " > " + maxSizeBytes + ").");*/
		boolean tooBig = tooManySearches || tooMuchMemory;
		return tooBig;
	}

	/**
	 * Checks if the search is too old to remain in cache.
	 *
	 * Only applies if maxSearchAgeSec >= 0.
	 *
	 * @param search the search to check
	 * @return true iff the search is too old
	 */
	private boolean searchTooOld(Job search) {
		boolean tooOld = maxSearchAgeSec >= 0 && search.ageInSeconds() > maxSearchAgeSec;
		//if (tooOld) logger.debug("Search is too old: " + search);
		return tooOld;
	}

	/**
	 * Return the maximum size of the cache to target, in bytes.
	 *
	 * @return targeted max. size of the cache in bytes, or -1 for no limit
	 */
	public long getMaxSizeBytes() {
		return maxSizeBytes;
	}

	/**
	 * Set the maximum size of the cache to target, in bytes.
	 *
	 * NOTE: the maximum size is checked based on a rough estimate of the
	 * memory consumed by each search. Also, the specified value may be exceeded
	 * because Search objects are added to the cache before the search is executed,
	 * so they grow in size. Choose a conservative size and monitor memory usage in
	 * practice.
	 *
	 * @param maxSizeBytes targeted max. size of the cache in bytes, or -1 for no limit
	 */
	public void setMaxSizeBytes(long maxSizeBytes) {
		this.maxSizeBytes = maxSizeBytes;
		removeOldSearches();
	}

	/**
	 * Return the maximum size of the cache in number of searches.
	 * @return the maximum size, or -1 for no limit
	 */
	public int getMaxNumberOfSearches() {
		return maxNumberOfSearches;
	}

	/**
	 * Set the maximum size of the cache in number of searches.
	 * @param maxSizeSearches the maximum size, or -1 for no limit
	 */
	public void setMaxSearchesToCache(int maxSizeSearches) {
		this.maxNumberOfSearches = maxSizeSearches;
		removeOldSearches();
	}

	/**
	 * Return the maximum age of a search in the cache.
	 *
	 * The age is defined as the period of time since the last access.
	 *
	 * @return the maximum age, or -1 for no limit
	 */
	public int getMaxSearchAgeSec() {
		return maxSearchAgeSec;
	}

	/**
	 * Set the maximum age of a search in the cache.
	 *
	 * The age is defined as the period of time since the last access.
	 *
	 * @param maxSearchAgeSec the maximum age, or -1 for no limit
	 */
	public void setMaxSearchAgeSec(int maxSearchAgeSec) {
		this.maxSearchAgeSec = maxSearchAgeSec;
	}

	public long getSizeBytes() {
		return calculateSizeBytes(cachedSearches.values());
	}

	public int getNumberOfSearches() {
		return cachedSearches.size();
	}

}
