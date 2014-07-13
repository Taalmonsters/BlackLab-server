package nl.inl.blacklab.server.search;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.inl.blacklab.analysis.BLDutchAnalyzer;
import nl.inl.blacklab.index.BLDefaultAnalyzer;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.queryParser.contextql.ContextualQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.CorpusQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.ParseException;
import nl.inl.blacklab.queryParser.corpusql.TokenMgrError;
import nl.inl.blacklab.queryParser.lucene.LuceneQueryParser;
import nl.inl.blacklab.search.CompleteQuery;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TextPattern;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.MemoryUtil;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;
import org.json.JSONArray;
import org.json.JSONObject;

public class SearchManager  {
	private static final Logger logger = Logger.getLogger(SearchManager.class);

	/**
	 * When the SearchManager was created. Used in logging to show ms since
	 * server start instead of all-time.
	 */
	long createdAt = System.currentTimeMillis();

	/** How long the server should wait for a quick answer when starting
	 * a nonblocking request. If the answer is found within this time,
	 * the client needs only one request even in nonblocking mode. */
	private int waitTimeInNonblockingModeMs;

	/** The minimum time to advise a client to
	 * wait before checking the status of a search again. */
	private int checkAgainAdviceMinimumMs;

	/** What number to divide the search time so far
	 * by to get the check again advice. E.g. if this is set
	 * to 5 (the default), if a search has been running for 10
	 * seconds, clients are advised to wait 2 seconds before
	 * checking the status again. */
	private int checkAgainAdviceDivider;

	/** Maximum context size allowed */
	private int maxContextSize;

	/** Maximum snippet size allowed */
	private int maxSnippetSize;

	/** Parameters involved in search */
	private List<String> searchParameterNames;

	/** Default values for request parameters */
	private Map<String, String> defaultParameterValues;

//	/** Run in debug mode or not? [no] */
//	private boolean debugMode;

	/** Default number of hits/results per page [20] */
	private int defaultPageSize;

	private Map<String, IndexParam> indexParam;

	/** The Searcher objects, one for each of the indices we can search. */
	private Map<String, Searcher> searchers = new HashMap<String, Searcher>();

	/** All running searches as well as recently run searches */
	private SearchCache cache;

	/** Keeps track of running jobs per user, so we can limit this. */
	private Map<String, Set<Job>> runningJobsPerUser = new HashMap<String, Set<Job>>();

	/** Default pattern language to use. [corpusql] */
	private String defaultPatternLanguage;

	/** Default filter language to use. [luceneql] */
	private String defaultFilterLanguage;

	/** Should requests be blocking by default? [yes] */
	private boolean defaultBlockingMode;

	/** Default number of words around hit. [5] */
	private int defaultContextSize;

	/** Minimum amount of free memory (MB) to start a new search. [50] */
	private int minFreeMemForSearchMegs;

	/** Maximum number of simultaneously running jobs started by the same user. [20]
	 *  Please note that a search may start 2-4 jobs, so don't set this too low. This
	 *  is just meant to prevent over-eager scripts and other abuse. Regular users
	 *  should never hit this limit. */
	private long maxRunningJobsPerUser;

	/** IP addresses for which debug mode will be turned on. */
	private Set<String> debugModeIps;

	/** The default output type, JSON or XML. */
	private DataFormat defaultOutputType;

	/** Which IPs are allowed to override the userId using a parameter
	 *  (for other IPs, the session id is the userId) */
	private Set<String> overrideUserIdIps;

	/** How long the client may used a cached version of the results we give them.
	 *  This is used to write HTTP cache headers. A value of an hour or so seems
	 *  reasonable. */
	private int clientCacheTimeSec;

	//private JSONObject properties;

	public SearchManager(JSONObject properties)  {
		logger.debug("SearchManager created");

		//this.properties = properties;
		JSONArray jsonDebugModeIps = properties.getJSONArray("debugModeIps");
		debugModeIps = new HashSet<String>();
		for (int i = 0; i < jsonDebugModeIps.length(); i++) {
			debugModeIps.add(jsonDebugModeIps.getString(i));
		}

		// Request properties
		JSONObject reqProp = properties.getJSONObject("requests");
		defaultOutputType = DataFormat.XML; // XML if nothing specified (because of browser's default Accept header)
		if (reqProp.has("defaultOutputType"))
			defaultOutputType = ServletUtil.getOutputTypeFromString(reqProp.getString("defaultOutputType"), DataFormat.XML);
		defaultPageSize = JsonUtil.getIntProp(reqProp, "defaultPageSize", 20);
		defaultPatternLanguage = JsonUtil.getProperty(reqProp, "defaultPatternLanguage", "corpusql");
		defaultFilterLanguage = JsonUtil.getProperty(reqProp, "defaultFilterLanguage", "luceneql");
		defaultBlockingMode = JsonUtil.getBooleanProp(reqProp, "defaultBlockingMode", true);
		defaultContextSize = JsonUtil.getIntProp(reqProp, "defaultContextSize", 5);
		maxContextSize = JsonUtil.getIntProp(reqProp, "maxContextSize", 20);
		maxSnippetSize = JsonUtil.getIntProp(reqProp, "maxSnippetSize", 100);
		JSONArray jsonOverrideUserIdIps = reqProp.getJSONArray("overrideUserIdIps");
		overrideUserIdIps = new HashSet<String>();
		for (int i = 0; i < jsonOverrideUserIdIps.length(); i++) {
			overrideUserIdIps.add(jsonOverrideUserIdIps.getString(i));
		}

		// Performance properties
		JSONObject perfProp = properties.getJSONObject("performance");
		minFreeMemForSearchMegs = JsonUtil.getIntProp(perfProp, "minFreeMemForSearchMegs", 50);
		maxRunningJobsPerUser = JsonUtil.getIntProp(perfProp, "maxRunningJobsPerUser", 20);
		checkAgainAdviceMinimumMs = JsonUtil.getIntProp(perfProp, "checkAgainAdviceMinimumMs", 200);
		checkAgainAdviceDivider = JsonUtil.getIntProp(perfProp, "checkAgainAdviceDivider", 5);
		waitTimeInNonblockingModeMs = JsonUtil.getIntProp(perfProp, "waitTimeInNonblockingModeMs", 100);
		clientCacheTimeSec = JsonUtil.getIntProp(perfProp, "clientCacheTimeSec", 3600);

		// Cache properties
		JSONObject cacheProp = perfProp.getJSONObject("cache");

		// Find the indices
		indexParam = new HashMap<String, IndexParam>();
		JSONObject indicesMap = properties.getJSONObject("indices");
		Iterator<?> it = indicesMap.keys();
		while (it.hasNext()) {
			String indexName = (String)it.next();
			JSONObject indexConfig = indicesMap.getJSONObject(indexName);

			File dir = JsonUtil.getFileProp(indexConfig, "dir", null);
			if (dir == null || !dir.exists()) {
				logger.error("Index directory for index '" + indexName + "' does not exist: " + dir);
				continue;
			}

			String pid = JsonUtil.getProperty(indexConfig, "pid", "");
			if (pid.length() == 0) {
				logger.warn("No pid given for index '" + indexName + "'; using Lucene doc ids.");
			}

			boolean mayViewContent = JsonUtil.getBooleanProp(indexConfig, "mayViewContent", false);

			indexParam.put(indexName, new IndexParam(dir, pid, mayViewContent));
		}
		if (indexParam.size() == 0)
			throw new RuntimeException("Configuration error: no indices available. Specify indexNames (space-separated) and indexDir_<name> for each index!");

		// Keep a list of searchparameters.
		searchParameterNames = Arrays.asList(
				"resultsType", "patt", "pattlang", "pattfield", "filter", "filterlang",
				"sort", "group", "viewgroup", "collator", "first", "number", "wordsaroundhit",
				"hitstart", "hitend", "facets");

		// Set up the parameter default values
		defaultParameterValues = new HashMap<String, String>();
		defaultParameterValues.put("filterlang", defaultFilterLanguage);
		defaultParameterValues.put("pattlang", defaultPatternLanguage);
		defaultParameterValues.put("sort", "");
		defaultParameterValues.put("group", "");
		defaultParameterValues.put("viewgroup", "");
		defaultParameterValues.put("first", "0");
		defaultParameterValues.put("hitstart", "0");
		defaultParameterValues.put("hitend", "1");
		defaultParameterValues.put("number", "" + defaultPageSize);
		defaultParameterValues.put("block", defaultBlockingMode ? "yes" : "no");
		defaultParameterValues.put("wordsaroundhit", "" + defaultContextSize);

		// Start with empty cache
		cache = new SearchCache(cacheProp);
	}

	public List<String> getSearchParameterNames() {
		return searchParameterNames;
	}

	/**
	 * Get the Searcher object for the specified index.
	 * @param indexName the index we want to search
	 * @return the Searcher object for that index
	 * @throws IndexOpenException if not found or open error
	 */
	public synchronized Searcher getSearcher(String indexName) throws IndexOpenException {
		if (searchers.containsKey(indexName))  {
			return searchers.get(indexName);
		}
		File indexDir = getIndexDir(indexName);
		if (indexDir == null)  {
			throw new IndexOpenException("Index " + indexName + " not found");
		}
		Searcher searcher;
		try {
			logger.debug("Opening index '" + indexName + "', dir = " + indexDir);
			searcher = Searcher.open(indexDir);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IndexOpenException("Could not open index '" + indexName + "'", e);
		}
		searchers.put(indexName, searcher);
		return searcher;
	}

	/** Get the index directory based on index name
	 *
	 * @param indexName short name of the index
	 * @return the index directory, or null if not found
	 */
	private File getIndexDir(String indexName) {
		IndexParam p = indexParam.get(indexName);
		if (p == null)
			return null;
		return p.getDir();
	}

	/** Get the persistent identifier field for an index.
	 *
	 * @param indexName the index
	 * @return the persistent identifier field, or empty if none (use Lucene doc id)
	 */
	public String getIndexPidField(String indexName) {
		IndexParam p = indexParam.get(indexName);
		if (p == null)
			return null;
		return p.getPidField();
	}

	/**
	 * Get the pid for the specified document
	 * @param indexName index name
	 * @param luceneDocId Lucene document id
	 * @param document the document object
	 * @return the pid string (or Lucene doc id in string form if index has no pid field)
	 */
	public String getDocumentPid(String indexName, int luceneDocId, Document document) {
		String pidField = getIndexPidField(indexName);
		if (pidField.length() == 0)
			return "" + luceneDocId;
		return document.get(pidField);
	}

	/**
	 * Get the Lucene Document id given the pid
	 * @param indexName index name
	 * @param pid the pid string (or Lucene doc id if we don't use a pid)
	 * @return the document id, or -1 if it doesn't exist
	 * @throws IndexOpenException
	 */
	public int getLuceneDocIdFromPid(String indexName, String pid) throws IndexOpenException {
		String pidField = getIndexPidField(indexName);
		Searcher searcher = getSearcher(indexName);
		if (pidField.length() == 0) {
			int luceneDocId;
			try {
				luceneDocId = Integer.parseInt(pid);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Pid must be a Lucene doc id, but it's not a number: " + pid);
			}
			return luceneDocId;
		}
		boolean lowerCase = false; // HACK in case pid field is incorrectly lowercased
		DocResults docResults;
		while (true) {
			String p = lowerCase ? pid.toLowerCase() : pid;
			TermQuery documentFilterQuery = new TermQuery(new Term(pidField, p));
			docResults = searcher.queryDocuments(documentFilterQuery);
			if (docResults.size() > 1) {
				// HACK: temporarily turned off because some documents are indexed twice..
				//throw new IllegalArgumentException("Pid must uniquely identify a document, but it has " + docResults.size() + " hits: " + pid);
			}
			if (docResults.size() == 0) {
				if (lowerCase)
					return -1; // tried with and without lowercasing; doesn't exist
				lowerCase = true; // try lowercase now
			} else {
				// size == 1, found!
				break;
			}
		}
		return docResults.get(0).getDocId();
	}

	/**
	 * Return the list of indices available for searching.
	 * @return the list of index names
	 */
	public Collection<String> getAvailableIndices() {
		return indexParam.keySet();
	}

	public JobWithHits searchHits(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort", "doc-pid");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobHitsSorted");
			return (JobHitsSorted)search(userId, parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or it's cached wrong
		parBasic.put("jobclass", "JobHits");
		return (JobHits)search(userId, parBasic);
	}

	public JobWithDocs searchDocs(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobDocsSorted");
			return (JobDocsSorted)search(userId, parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or it's cached wrong
		parBasic.put("jobclass", "JobDocs");
		return (JobDocs)search(userId, parBasic);
	}

	public JobHitsWindow searchHitsWindow(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort", "first", "number", "wordsaroundhit");
		parBasic.put("jobclass", "JobHitsWindow");
		return (JobHitsWindow)search(userId, parBasic);
	}

	public JobDocsWindow searchDocsWindow(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort", "first", "number", "wordsaroundhit");
		parBasic.put("jobclass", "JobDocsWindow");
		return (JobDocsWindow)search(userId, parBasic);
	}

	public JobHitsTotal searchHitsTotal(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobHitsTotal");
		return (JobHitsTotal)search(userId, parBasic);
	}

	public JobDocsTotal searchDocsTotal(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobDocsTotal");
		return (JobDocsTotal)search(userId, parBasic);
	}

	public JobHitsGrouped searchHitsGrouped(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "group", "sort");
		parBasic.put("jobclass", "JobHitsGrouped");
		return (JobHitsGrouped)search(userId, parBasic);
	}

	public JobDocsGrouped searchDocsGrouped(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "group", "sort");
		parBasic.put("jobclass", "JobDocsGrouped");
		return (JobDocsGrouped)search(userId, parBasic);
	}

	public JobFacets searchFacets(String userId, SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("facets", "indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobFacets");
		return (JobFacets)search(userId, parBasic);
	}

	/**
	 * Start a new search or return an existing Search object corresponding
	 * to these search parameters.
	 *
	 * @param userId user creating the job
	 * @param searchParameters the search parameters
	 * @param blockUntilFinished if true, wait until the search finishes; otherwise, return immediately
	 * @return a Search object corresponding to these parameters
	 * @throws QueryException if the query couldn't be executed
	 * @throws IndexOpenException if the index couldn't be opened
	 * @throws InterruptedException if the search thread was interrupted
	 */
	private Job search(String userId, SearchParameters searchParameters) throws IndexOpenException, QueryException, InterruptedException {
		// Search the cache / running jobs for this search, create new if not found.
		boolean performSearch = false;
		Job search;
		synchronized(this) {
			search = cache.get(searchParameters);
			if (search == null) {
				// Not found in cache

				// Do we have enough memory to start a new search?
				long freeMegs = MemoryUtil.getFree() / 1000000;
				if (freeMegs < minFreeMemForSearchMegs) {
					cache.removeOldSearches(); // try to free up space for next search
					logger.warn("Can't start new search, not enough memory (" + freeMegs + "M < " + minFreeMemForSearchMegs + "M)");
					throw new QueryException("SERVER_BUSY", "The server is under heavy load right now. Please try again later.");
				}
				logger.debug("Enough free memory: " + (freeMegs/1000000) + "M");

				// Is this user allowed to start another search?
				int numRunningJobs = 0;
				Set<Job> runningJobs = runningJobsPerUser.get(userId);
				Set<Job> newRunningJobs = new HashSet<Job>();
				if (runningJobs != null) {
					for (Job job: runningJobs) {
						if (!job.finished()) {
							numRunningJobs++;
							newRunningJobs.add(job);
						}
					}
				}
				if (numRunningJobs >= maxRunningJobsPerUser) {
					// User has too many running jobs. Can't start another one.
					runningJobsPerUser.put(userId, newRunningJobs); // refresh the list
					logger.warn("Can't start new search, user already has " + numRunningJobs + " jobs running.");
					throw new QueryException("TOO_MANY_JOBS", "You already have too many running searches. Please wait for some previous searches to complete before starting new ones.");
				}

				// Create a new search object with these parameters and place it in the cache
				search = Job.create(this, userId, searchParameters);
				cache.put(search);

				// Update running jobs
				newRunningJobs.add(search);
				runningJobsPerUser.put(userId, newRunningJobs);

				performSearch = true;
			}
		}

		if (performSearch) {
			// Start the search, waiting a short time in case it's a fast search
			search.perform(waitTimeInNonblockingModeMs);
		}

		// If the search thread threw an exception, rethrow it now.
		if (search.threwException()) {
			search.rethrowException();
		}

		return search;
	}
	
	public long getMinFreeMemForSearchMegs() {
		return minFreeMemForSearchMegs;
	}

	public String getParameterDefaultValue(String paramName) {
		String defVal = defaultParameterValues.get(paramName);
		/*if (defVal == null) {
			defVal = "";
			defaultParameterValues.put(paramName, defVal);
		}*/
		return defVal;
	}

	public static boolean strToBool(String value) throws IllegalArgumentException {
		if (value.equals("true") || value.equals("1") || value.equals("yes") || value.equals("on"))
			return true;
		if (value.equals("false") || value.equals("0") || value.equals("no") || value.equals("off"))
			return false;
		throw new IllegalArgumentException("Cannot convert to boolean: " + value);
	}

	public static int strToInt(String value) throws IllegalArgumentException {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Cannot convert to int: " + value);
		}
	}

	/**
	 * Construct a simple error response object.
	 *
	 * @param code (string) error code
	 * @param msg the error message
	 * @return the data object representing the error message
	 */
	public static DataObject errorObject(String code, String msg) {
		DataObjectMapElement error = new DataObjectMapElement();
		error.put("code", new DataObjectString(code));
		error.put("message", new DataObjectString(msg));
		DataObjectMapElement rv = new DataObjectMapElement();
		rv.put("error", error);
		return rv;
	}

	public TextPattern parsePatt(String indexName, String pattern, String language) throws QueryException {
		return parsePatt(indexName, pattern, language, true);
	}

	public TextPattern parsePatt(String indexName, String pattern, String language, boolean required) throws QueryException {
		if (pattern == null || pattern.length() == 0) {
			if (required)
				throw new QueryException("NO_PATTERN_GIVEN", "Text search pattern required. Please specify 'patt' parameter.");
			return null; // not required, ok
		}

		if (language.equals("corpusql")) {
			try {
				return CorpusQueryLanguageParser.parse(pattern);
			} catch (ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in CorpusQL pattern: " + e.getMessage());
			} catch (TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in CorpusQL pattern: " + e.getMessage());
			}
		} else if (language.equals("contextql")) {
			try {
				CompleteQuery q = ContextualQueryLanguageParser.parse(pattern);
				return q.getContentsQuery();
			} catch (nl.inl.blacklab.queryParser.contextql.TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in ContextQL pattern: " + e.getMessage());
			} catch (nl.inl.blacklab.queryParser.contextql.ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in ContextQL pattern: " + e.getMessage());
			}
		} else if (language.equals("luceneql")) {
			try {
				String field = getSearcher(indexName).getIndexStructure().getMainContentsField().getName();
				LuceneQueryParser parser = new LuceneQueryParser(Version.LUCENE_42, field, new BLDefaultAnalyzer());
				return parser.parse(pattern);
			} catch (IndexOpenException e) {
				throw new RuntimeException(e); // should never happen at this point
			} catch (nl.inl.blacklab.queryParser.lucene.ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in LuceneQL pattern: " + e.getMessage());
			} catch (nl.inl.blacklab.queryParser.lucene.TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in LuceneQL pattern: " + e.getMessage());
			}
		}

		throw new QueryException("UNKNOWN_PATT_LANG", "Unknown pattern language '" + language + "'. Supported: corpusql, contextql, luceneql.");
	}

	public static Query parseFilter(String filter, String filterLang) throws QueryException {
		return parseFilter(filter, filterLang, false);
	}

	public static Query parseFilter(String filter, String filterLang, boolean required) throws QueryException {
		if (filter == null || filter.length() == 0) {
			if (required)
				throw new QueryException("NO_FILTER_GIVEN", "Document filter required. Please specify 'filter' parameter.");
			return null; // not required
		}

		if (filterLang.equals("luceneql")) {
			try {
				QueryParser parser = new QueryParser(Version.LUCENE_42, "", new BLDutchAnalyzer());
				Query query = parser.parse(filter);
				return query;
			} catch (org.apache.lucene.queryparser.classic.ParseException e) {
				throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing LuceneQL filter query: " + e.getMessage());
			} catch (org.apache.lucene.queryparser.classic.TokenMgrError e) {
				throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing LuceneQL filter query: " + e.getMessage());
			}
		} else if (filterLang.equals("contextql")) {
			try {
				CompleteQuery q = ContextualQueryLanguageParser.parse(filter);
				return q.getFilterQuery();
			} catch (nl.inl.blacklab.queryParser.contextql.TokenMgrError e) {
				throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing ContextQL filter query: " + e.getMessage());
			} catch (nl.inl.blacklab.queryParser.contextql.ParseException e) {
				throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing ContextQL filter query: " + e.getMessage());
			}
		}

		throw new QueryException("UNKNOWN_FILTER_LANG", "Unknown filter language '" + filterLang + "'. Supported: luceneql, contextql.");
	}

	public int getCheckAgainAdviceMinimumMs() {
		return checkAgainAdviceMinimumMs;
	}

	public boolean isDebugMode(String ip) {
		return debugModeIps.contains(ip);
	}

	static void debugWait() {
		// Fake extra search time
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public SearchCache getCache() {
		return cache;
	}

	public boolean mayViewContents(String indexName, Document document) {
		IndexParam p = indexParam.get(indexName);
		if (p == null)
			return false;
		return p.mayViewContents();
	}

	public DataFormat getContentsFormat(String indexName) {
		return DataFormat.XML; // could be made configurable
	}

	public int getMaxContextSize() {
		return maxContextSize;
	}

	public DataObject getCacheStatusDataObject() {
		return cache.getCacheStatusDataObject();
	}

	public DataObject getCacheContentsDataObject() {
		return cache.getContentsDataObject();
	}

	public int getMaxSnippetSize() {
		return maxSnippetSize;
	}

	public boolean mayOverrideUserId(String ip) {
		return overrideUserIdIps.contains(ip);
	}

	public DataFormat getDefaultOutputType() {
		return defaultOutputType;
	}

	public int getClientCacheTimeSec() {
		return clientCacheTimeSec;
	}

	/**
	 * Give advice for how long to wait to check the status of a search.
	 * @param search the search you want to check the status of
	 * @return how long you should wait before asking again
	 */
	public int getCheckAgainAdviceMs(Job search) {

		// Simple advice algorithm: the longer the search
		// has been running, the less frequently the client
		// should check its progress. Just divide the search time by
		// 5 with a configured minimum.
		int runningFor = search.ageInSeconds();
		int checkAgainAdvice = Math.min(checkAgainAdviceMinimumMs, runningFor * 1000 / checkAgainAdviceDivider);

		return checkAgainAdvice;
	}

}
