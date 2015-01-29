package nl.inl.blacklab.server.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.ConfigurationException;
import nl.inl.util.MemoryUtil;
import nl.inl.util.ThreadPriority.Level;
import nl.inl.util.json.JSONArray;
import nl.inl.util.json.JSONObject;

import org.apache.log4j.Logger;

public class SearchCache {
	private static final Logger logger = Logger.getLogger(SearchCache.class);

	private static final int MAX_PAUSED = 10; // TODO: configurable

	/** Max time searches are allowed to run (5 minutes) */
	public static int maxSearchTimeSec = 5 * 60;

	/** @param maxSearchTimeSec Max time searches are allowed to run (default: 300s == 5 minutes) */
	public static void setMaxSearchTimeSec(int maxSearchTimeSec) {
		SearchCache.maxSearchTimeSec = maxSearchTimeSec;
	}

	/** The cached search objects. */
	private Map<SearchParameters, Job> cachedSearches;

	/** Maximum size in MB to target, or -1 for no limit. NOT IMPLEMENTED YET. */
	private long maxSizeMegs = -1;

	/** Maximum number of searches to cache, or -1 for no limit. Defaults to (a fairly low) 20.*/
	private int maxNumberOfJobs = 20;

	/** Maximum age of a cached search in seconds. May be exceeded because it is only cleaned up when
	 *  adding new searches. Defaults to one hour. */
	private int maxJobAgeSec = 3600;

	/** (Estimated) size of the cache. Only updated in removeOldSearches, so may not
	 * always be accurate. */
	private long cacheSizeBytes;

	/** How much free memory we should try to target when cleaning the cache. */
	private long minFreeMemTargetMegs;

	/** If we're below target mem, how many jobs should we get rid of each time we add something to the cache? */
	private int numberOfJobsToPurgeWhenBelowTargetMem;

	/**
	 * Initialize the cache.
	 *
	 * @param settings cache settings
	 */
	public SearchCache(JSONObject settings) {
		cachedSearches = new HashMap<SearchParameters, Job>();
		maxJobAgeSec = JsonUtil.getIntProp(settings, "maxJobAgeSec", 3600);
		maxNumberOfJobs = JsonUtil.getIntProp(settings, "maxNumberOfJobs", 20);
		maxSizeMegs = JsonUtil.getIntProp(settings, "maxSizeMegs", -1);
		minFreeMemTargetMegs = JsonUtil.getIntProp(settings, "targetFreeMemMegs", 100);
		numberOfJobsToPurgeWhenBelowTargetMem = JsonUtil.getIntProp(settings, "numberOfJobsToPurgeWhenBelowTargetMem", 100);
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
		if (maxNumberOfJobs <= 0)
			return;
		
		removeOldSearches();
		
		performLoadManagement(search);

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
		search.incrRef();
	}
	
	/**
	 * Remove all cache entries for the specified index.
	 * 
	 * @param indexName the index
	 */
	public void clearCacheForIndex(String indexName) {
		// Iterate over the entries and remove the ones in the specified index
		Iterator<Map.Entry<SearchParameters, Job>> it = cachedSearches.entrySet().iterator();
		while (it.hasNext()) {
			Entry<SearchParameters, Job> entry = it.next();
			if (entry.getKey().getString("indexname").equals(indexName)) {
				entry.getValue().decrRef();
				it.remove();
			}
		}
	}

	/**
	 * Get rid of all the cached Searches.
	 */
	public void clearCache() {
		for (Job cachedSearch: cachedSearches.values()) {
			cachedSearch.decrRef();
		}
		cachedSearches.clear();
		logger.debug("Cache cleared.");
	}

	/**
	 * If the cache exceeds the given parameters, clean it up by
	 * removing less recently used searches.
	 */
	void removeOldSearches() {
		
		// Sort cache by last access time
		List<Job> lastAccessOrder = new ArrayList<Job>(cachedSearches.values());
		Collections.sort(lastAccessOrder); // put stalest first
		
		calculateSizeBytes(lastAccessOrder);
		
		// If we're low on memory, always remove a few searches from cache.
		int minSearchesToRemove = 0;
		long freeMegs = MemoryUtil.getFree() / 1000000;
		if (freeMegs < minFreeMemTargetMegs) {
			minSearchesToRemove = numberOfJobsToPurgeWhenBelowTargetMem; // arbitrary, number but will keep on being removed every call until enough free mem has been reclaimed
			logger.debug("Not enough free mem, will remove some searches.");
		}

		// Get rid of old searches
		boolean lookAtCacheSizeAndSearchAccessTime = true;
		for (Job search: lastAccessOrder) {
			if (!search.finished() && search.executionTime() > maxSearchTimeSec) {
				// Search is taking too long. Cancel it.
				logger.debug("Search is taking too long, cancelling: " + search);
				abortSearch(search);

			} else {
				boolean removeBecauseOfCacheSizeOrAge = false;
				if (lookAtCacheSizeAndSearchAccessTime) {
					boolean isCacheTooBig = cacheTooBig();
					boolean isSearchTooOld = false;
					if (!isCacheTooBig)
						isSearchTooOld = searchTooOld(search);
					removeBecauseOfCacheSizeOrAge = isCacheTooBig || isSearchTooOld;
				}
				if (minSearchesToRemove > 0 || removeBecauseOfCacheSizeOrAge) {
					// Search is too old or cache is too big. Keep removing searches until that's no longer the case
					//logger.debug("Remove from cache: " + search);
					removeFromCache(search);
					
					minSearchesToRemove--;
				} else {
					// Cache is no longer too big and these searches are not too old. Stop checking that,
					// just check for long-running searches
					lookAtCacheSizeAndSearchAccessTime = false;
				}
			}
		}
		// NOTE: we used to hint the Java GC to run, but this caused severe
		// slowdowns. It's better to rely on the incremental garbage collection.
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
		boolean tooManySearches = maxNumberOfJobs >= 0
				&& cachedSearches.size() > maxNumberOfJobs;
		long cacheSizeMegs = cacheSizeBytes / 1000000;
		boolean tooMuchMemory = maxSizeMegs >= 0 && cacheSizeMegs > maxSizeMegs;
		return tooManySearches || tooMuchMemory;
	}

	/**
	 * Checks if the search is too old to remain in cache.
	 *
	 * Only applies if maxJobAgeSec >= 0.
	 *
	 * @param search the search to check
	 * @return true iff the search is too old
	 */
	private boolean searchTooOld(Job search) {
		boolean tooOld = maxJobAgeSec >= 0 && search.cacheAge() > maxJobAgeSec;
		return tooOld;
	}

	/**
	 * Return the maximum size of the cache to target, in bytes.
	 *
	 * @return targeted max. size of the cache in bytes, or -1 for no limit
	 */
	public long getMaxSizeBytes() {
		return maxSizeMegs;
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
		this.maxSizeMegs = maxSizeBytes;
		removeOldSearches();
	}

	/**
	 * Return the maximum size of the cache in number of searches.
	 * @return the maximum size, or -1 for no limit
	 */
	public int getMaxJobsToCache() {
		return maxNumberOfJobs;
	}

	/**
	 * Set the maximum size of the cache in number of searches.
	 * @param maxJobs the maximum size, or -1 for no limit
	 */
	public void setMaxJobsToCache(int maxJobs) {
		this.maxNumberOfJobs = maxJobs;
		removeOldSearches();
	}

	/**
	 * Return the maximum age of a search in the cache.
	 *
	 * The age is defined as the period of time since the last access.
	 *
	 * @return the maximum age, or -1 for no limit
	 */
	public int getMaxJobAgeSec() {
		return maxJobAgeSec;
	}

	/**
	 * Set the maximum age of a search in the cache.
	 *
	 * The age is defined as the period of time since the last access.
	 *
	 * @param maxJobAgeSec the maximum age, or -1 for no limit
	 */
	public void setMaxJobAgeSec(int maxJobAgeSec) {
		this.maxJobAgeSec = maxJobAgeSec;
	}

	public long getSizeBytes() {
		return calculateSizeBytes(cachedSearches.values());
	}

	public int getNumberOfSearches() {
		return cachedSearches.size();
	}

	public int numberOfRunningSearches() {
		int n = 0;
		for (Job job: cachedSearches.values()) {
			if (!job.finished() && !job.isWaitingForOtherJob()) {
				n++;
			}
		}
		return n;
	}
	
	private int numberOfPausedSearches() {
		int n = 0;
		for (Job job: cachedSearches.values()) {
			if (job.getPriorityLevel() == Level.PAUSED) {
				n++;
			}
		}
		return n;
	}

	public void setMinFreeMemTargetBytes(long minFreeMemTargetBytes) {
		this.minFreeMemTargetMegs = minFreeMemTargetBytes;
	}

	public DataObject getCacheStatusDataObject() {
		DataObjectMapElement doCache = new DataObjectMapElement();
		doCache.put("maxSizeBytes", getMaxSizeBytes());
		doCache.put("maxNumberOfSearches", getMaxJobsToCache());
		doCache.put("maxSearchAgeSec", getMaxJobAgeSec());
		doCache.put("sizeBytes", getSizeBytes());
		doCache.put("numberOfSearches", getNumberOfSearches());
		return doCache;
	}

	public DataObject getContentsDataObject() {
		DataObjectList doCacheContents = new DataObjectList("job");
		for (Job job: cachedSearches.values()) {
			doCacheContents.add(job.toDataObject());
		}
		return doCacheContents;
	}
	
	/**
	 * What we can do to a query in response to the server load.
	 */
	enum ServerLoadQueryAction {
		NONE,         // no action
		DISCARD,      // discard results from cache
		LOWER_PRIO,   // continue searching with lower thread priority
		PAUSE_SEARCH, // pause search
		PAUSE_COUNT,  // pause count
		PAUSE_NEW,    // abort new search
		RESUME,       // resume paused search
		ABORT_SEARCH, // abort search / refuse to start new search
		ABORT_COUNT,  // abort count
		ABORT_NEW,    // abort new search
	}
	
	/**
	 * An object describing what to do for a certain server load.
	 * Defined in the blacklab-server.json config file under
	 * performance.serverLoadBehaviour.
	 */
	class ServerLoadState implements Comparable<ServerLoadState> {
		
		/** State behaviour: matchers in order of importance */
		List<QueryStateMatcher> matchers = new ArrayList<QueryStateMatcher>();
		
		public List<QueryStateMatcher> getMatchers() {
			return matchers;
		}

		public List<ServerLoadQueryAction> getActions() {
			return actions;
		}

		/** Minimum number of searches to reach this state */
		List<ServerLoadQueryAction> actions = new ArrayList<ServerLoadQueryAction>();

		/** State name */
		private String name;

		/** Minimum number of searches to reach this state */
		private int minSearches;

		public ServerLoadState(String name, int minSearches) {
			this.name = name;
			this.minSearches = minSearches;
		}
		
		public String getName() {
			return name;
		}
		
		/**
		 * Add a behaviour rule to this state.
		 * @param matcher for which searches to invoke this rule
		 * @param action what to do with the search if the rule matches
		 */
		public void add(QueryStateMatcher matcher, ServerLoadQueryAction action) {
			matchers.add(matcher);
			actions.add(action);
		}

		/**
		 * Default sort order: descending on min. number of searches.
		 * 
		 * This put the heaviest load first.
		 */
		@Override
		public int compareTo(ServerLoadState o) {
			return o.minSearches - minSearches;
		}

		public int getMinSearches() {
			return minSearches;
		}

		@Override
		public String toString() {
			return name;
		}
		
	}
	
	/** Our server load states from heaviest to lightest */
	List<ServerLoadState> serverLoadStates = new ArrayList<ServerLoadState>();
	
	/** Our current server load */
	ServerLoadState currentLoadState;

	public void setServerLoadStates(JSONArray jsonStates) throws ConfigurationException {
		for (int i = 0; i < jsonStates.length(); i++) {
			JSONObject jsonState = jsonStates.getJSONObject(i);
			String name = jsonState.getString("name");
			int minSearches = JsonUtil.getIntProp(jsonState, "minSearches", 0);
			ServerLoadState state = new ServerLoadState(name, minSearches);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "discardResults", "cached 1800")), ServerLoadQueryAction.DISCARD);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "lowerPriority", "never")), ServerLoadQueryAction.LOWER_PRIO);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "pauseCount", "never")), ServerLoadQueryAction.PAUSE_COUNT);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "pauseSearch", "never")), ServerLoadQueryAction.PAUSE_SEARCH);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "resumeSearch", "paused 60")), ServerLoadQueryAction.RESUME);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "abortCount", "never")), ServerLoadQueryAction.ABORT_COUNT);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "abortSearch", "never")), ServerLoadQueryAction.ABORT_SEARCH);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "queueNewSearches", "never")), ServerLoadQueryAction.PAUSE_NEW);
			state.add(new QueryStateMatcher(JsonUtil.getProperty(jsonState, "refuseSearch", "never")), ServerLoadQueryAction.ABORT_NEW);
			serverLoadStates.add(state);
		}
		Collections.sort(serverLoadStates);
	}
	
	/**
	 * Figure out what load state the server is currently in.
	 * Note that no state may match, in which case currentLoadState will
	 * be set to null.
	 */
	public void determineCurrentLoad() {
		// serverLoadStates is sorted by minSearches descending,
		// so the first matching state is the current one
		int nSearches = numberOfRunningSearches();
		currentLoadState = null;
		for (ServerLoadState state: serverLoadStates) {
			// If if matches, or it's the last state (automatch)
			if (nSearches >= state.getMinSearches()) {
				currentLoadState = state;
				return;
			}
		}
	}

	/**
	 * Evaluate what we need to do (if anything) with each search given the 
	 * current server load.
	 * 
	 * @param newSearch the new search just started, or null if none.
	 */
	void performLoadManagement(Job newSearch) {
		if (serverLoadStates == null || serverLoadStates.size() == 0) {
			// Not configured. Don't activate the load manager stuff.
			return;
		}
		
		logger.debug("LOADMGR: START");
		determineCurrentLoad();
		if (currentLoadState == null) {
			logger.debug("LOADMGR: we're not in any of the states! => DONE");
			return;
		}
		boolean stateChanged;
		do {
			stateChanged = false;
			
			List<QueryStateMatcher> matchers = currentLoadState.getMatchers();
			List<ServerLoadQueryAction> actions = currentLoadState.getActions();
			
			// Iterate over the rules one by one, and for each rule,
			// see if it applies to any of our searches. If one does, perform
			// the action, then see if the state changed. If so, restart the
			// process. If we get through the whole thing without any state
			// changes, we're done.
			for (int i = 0; i < matchers.size() && !stateChanged; i++)  {
				QueryStateMatcher m = matchers.get(i);
				List<Job> searches = new ArrayList<Job>(cachedSearches.values());
				for (Job search: searches) {
					ServerLoadQueryAction action = actions.get(i);
					if (action == null)
						throw new RuntimeException("action == null");
					boolean isCountRule = action == ServerLoadQueryAction.PAUSE_COUNT || action == ServerLoadQueryAction.ABORT_COUNT;
					boolean isCountQuery = (search instanceof JobHitsTotal) || (search instanceof JobDocsTotal);
					boolean isSearchRule = action == ServerLoadQueryAction.PAUSE_SEARCH || action == ServerLoadQueryAction.ABORT_SEARCH;
					boolean isNewQueryRule = action == ServerLoadQueryAction.PAUSE_NEW || action == ServerLoadQueryAction.ABORT_NEW;
					boolean isNewQuery = search == newSearch;
					if (isCountRule && !isCountQuery ||
						isSearchRule && isCountQuery ||
						isNewQueryRule && !isNewQuery) {
						// This rule does not apply to this search. Skip over it.
						continue;
					}
					// See if this search matches this rule.
					if (m.matches(search)) {
						// Yes. Apply the action.
						logger.debug("Match: " + m.toString() + " (" + m.explainMatch(search) + ")");
						applyAction(search, action);
						
						// Did this action affect the current load state?
						// (NB lower/raise priority cannot affect the load state)
						if (action != ServerLoadQueryAction.LOWER_PRIO && action != ServerLoadQueryAction.NONE) {
							// See what our current load state is, and if it has changed.
							ServerLoadState oldLoadState = currentLoadState;
							determineCurrentLoad();
							if (currentLoadState == null) {
								logger.debug("LOADMGR: we're not in any of the states! => DONE");
								return; // no matching state, so nothing left to do
							}
							if (oldLoadState != currentLoadState) {
								// We've gone to a different state; restart the process.
								stateChanged = true;
								logger.debug("LOADMGR: changed from state " + oldLoadState + " to " + currentLoadState);
							}
						}
					}
				}
			}
		} while(stateChanged); // continue until there's nothing more to do 
		logger.debug("LOADMGR: DONE");
	}

	/**
	 * Apply one of the load managing actions to a search.
	 * 
	 * @param search the search
	 * @param action the action to apply
	 */
	private void applyAction(Job search, ServerLoadQueryAction action) {
		// See what to do with the current search
		switch(action) {
		case DISCARD:
			logger.debug("LOADMGR: Discarding from cache: " + search);
			removeFromCache(search);
			break;
		case LOWER_PRIO:
			if (search.getPriorityLevel() != Level.LOW) {
				logger.debug("LOADMGR: Lowering priority of: " + search);
				search.setPriorityLevel(Level.LOW);
			}
			break;
		case PAUSE_SEARCH:
		case PAUSE_COUNT:
		case PAUSE_NEW:
			if (search.getPriorityLevel() != Level.PAUSED && numberOfPausedSearches() < MAX_PAUSED) {
				logger.debug("LOADMGR: Pausing search: " + search);
				search.setPriorityLevel(Level.PAUSED);
			}
			break;
		case RESUME:
			if (search.getPriorityLevel() == Level.PAUSED) {
				logger.debug("LOADMGR: Resuming search: " + search);
				search.setPriorityLevel(Level.NORMAL);
			}
			break;
		case ABORT_SEARCH:
		case ABORT_COUNT:
		case ABORT_NEW:
			if (!search.finished()) {
				// TODO: Maybe we should blacklist certain searches for a time?
				logger.warn("LOADMGR: Aborting search: " + search);
				abortSearch(search);
			}
			break;
		case NONE:
			// Make sure it's running at normal priority (or paused, which is also ok)
			if (search.getPriorityLevel() != Level.LOW) {
				logger.debug("LOADMGR: Raising priority of: " + search);
				search.setPriorityLevel(Level.NORMAL);
			}
			break;
		}
	}
	
	private void removeFromCache(Job search) {
		cachedSearches.remove(search.getParameters());
		search.decrRef();
		cacheSizeBytes -= search.estimateSizeBytes();
	}

	private void abortSearch(Job search) {
		search.cancelJob();
		removeFromCache(search);
	}
}
