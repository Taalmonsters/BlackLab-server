package nl.inl.blacklab.server.search;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import nl.inl.blacklab.analysis.BLDutchAnalyzer;
import nl.inl.blacklab.queryParser.corpusql.CorpusQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.ParseException;
import nl.inl.blacklab.queryParser.corpusql.TokenMgrError;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TextPattern;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.PropertiesUtil;

import org.apache.log4j.Logger;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.Version;

public class SearchManager  {
	private static final Logger logger = Logger.getLogger(SearchManager.class);

	/** How long the server should wait for a quick answer when starting
	 * a nonblocking request. If the answer is found within this time,
	 * the client needs only one request even in nonblocking mode. */
	public int waitTimeInNonblockingModeMs;

	/** The default advice to give for when to check status of search again.
	 * TODO the longer a search has been running, the
	 *   higher the advised wait time should be.
	 */
	public int defaultCheckAgainAdviceMs;

	/** Maximum context size allowed */
	public int maxContextSize;

	/** Parameters involved in search */
	List<String> searchParameterNames;

	/** Default values for request parameters */
	Map<String, String> defaultParameterValues;

	/** Run in debug mode or not? */
	boolean debugMode;

	/** Default number of hits/results per page */
	private boolean defaultPageSize;

	/** Available indices and their directories */
	Map<String, File> indexDirs;

	/** The Searcher objects, one for each of the indices we can search. */
	Map<String, Searcher> searchers = new HashMap<String, Searcher>();

	/** All running searches as well as recently run searches */
	SearchCache cache;

	private String defaultPatternLanguage;

	private String defaultFilterLanguage;

	private boolean defaultBlockingMode;

	private int defaultContextSize;

	public SearchManager(Properties properties)  {
		logger.debug("SearchManager created");

		debugMode = PropertiesUtil.getBooleanProp(properties, "debugMode", false);
		defaultPageSize = PropertiesUtil.getBooleanProp(properties, "defaultPageSize", false);
		defaultPatternLanguage = properties.getProperty("defaultPatternLanguage", "corpusql");
		defaultFilterLanguage = properties.getProperty("defaultPatternLanguage", "luceneql");
		defaultBlockingMode = PropertiesUtil.getBooleanProp(properties, "defaultBlockingMode", true);
		defaultContextSize = PropertiesUtil.getIntProp(properties, "defaultContextSize", 5);
		maxContextSize = PropertiesUtil.getIntProp(properties, "maxContextSize", 20);
		int cacheMaxSearchAgeSec = PropertiesUtil.getIntProp(properties, "cacheMaxSearchAgeSec", 3600);
		int cacheMaxNumberOfSearches = PropertiesUtil.getIntProp(properties, "cacheMaxNumberOfSearches", 20);
		int cacheMaxSizeBytes = PropertiesUtil.getIntProp(properties, "cacheMaxSizeBytes", -1);
		defaultCheckAgainAdviceMs = PropertiesUtil.getIntProp(properties, "defaultCheckAgainAdviceMs", 200);
		waitTimeInNonblockingModeMs = PropertiesUtil.getIntProp(properties, "waitTimeInNonblockingModeMs", 100);

		String propIndexNames = properties.getProperty("indexNames");
		if (propIndexNames != null) {
			String[] indexNames = propIndexNames.trim().split("\\s+");
			indexDirs = new HashMap<String, File>();
			for (String indexName: indexNames) {
				File dir = PropertiesUtil.getFileProp(properties, "indexDir_" + indexName);
				if (dir == null) {
					logger.error("No index directory given for index '" + indexName + "' (supply indexDir_" + indexName + " setting)");
					continue;
				}
				if (!dir.exists()) {
					logger.error("Index directory for index '" + indexName + "' does not exist: " + dir);
					continue;
				}
				indexDirs.put(indexName, dir);
			}
		}
		if (indexDirs.size() == 0)
			throw new RuntimeException("Configuration error: no indices available. Specify indexNames (space-separated) and indexDir_<name> for each index!");

		// Keep a list of searchparameters.
		searchParameterNames = Arrays.asList(
				"resultsType", "patt", "pattlang", "pattfield", "filter", "filterlang",
				"sort", "group", "collator", "first", "number", "wordsaroundhit");

		// Set up the parameter default values
		defaultParameterValues = new HashMap<String, String>();
		defaultParameterValues.put("filterlang", defaultFilterLanguage);
		defaultParameterValues.put("pattlang", defaultPatternLanguage);
		defaultParameterValues.put("first", "0");
		defaultParameterValues.put("number", "" + defaultPageSize);
		defaultParameterValues.put("block", defaultBlockingMode ? "yes" : "no");
		defaultParameterValues.put("wordsaroundhit", "" + defaultContextSize);

		// Start with empty cache
		cache = new SearchCache();
		cache.setMaxSearchAgeSec(cacheMaxSearchAgeSec);
		cache.setMaxSearchesToCache(cacheMaxNumberOfSearches);
		cache.setMaxSizeBytes(cacheMaxSizeBytes);
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
			searcher = Searcher.open(indexDir);
		} catch (Exception e) {
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
		return indexDirs.get(indexName);
	}

	/**
	 * Return the list of indices available for searching.
	 * @return the list of index names
	 */
	public Collection<String> getAvailableIndices() {
		return indexDirs.keySet();
	}

	public JobHits searchHits(SearchParameters par, boolean blockUntilFinished) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobHits");
		return (JobHits)search(parBasic, blockUntilFinished);
	}

	public JobHitsWindow searchHitsWindow(SearchParameters par, boolean blockUntilFinished) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "first", "number", "wordsaroundhit");
		parBasic.put("jobclass", "JobHitsWindow");
		return (JobHitsWindow)search(parBasic, blockUntilFinished);
	}

	public JobHitsTotal searchHitsTotal(SearchParameters par, boolean blockUntilFinished) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobHitsTotal");
		return (JobHitsTotal)search(parBasic, blockUntilFinished);
	}

	/**
	 * Start a new search or return an existing Search object corresponding
	 * to these search parameters.
	 *
	 * @param searchParameters the search parameters
	 * @param blockUntilFinished if true, wait until the search finishes; otherwise, return immediately
	 * @return a Search object corresponding to these parameters
	 * @throws QueryException if the query couldn't be executed
	 * @throws IndexOpenException if the index couldn't be opened
	 * @throws InterruptedException if the search thread was interrupted
	 */
	private Job search(SearchParameters searchParameters, boolean blockUntilFinished) throws IndexOpenException, QueryException, InterruptedException {
		// Search the cache / running jobs for this search
		Job search = cache.get(searchParameters);

		// Not found; create a new search object with these parameters and place it in the cache
		if (search == null) {
			search = Job.create(this, searchParameters);
			cache.put(search);

			// Start the search (and, depending on the block parameter,
			// wait for it to finish, or return immediately)
			search.perform(blockUntilFinished ? -1 : waitTimeInNonblockingModeMs);
		}

		// If the search thread threw an exception, rethrow it now.
		if (search.threwException()) {
			search.rethrowException();
		}

		return search;
	}

	public String getParameterDefaultValue(String paramName) {
		String defVal = defaultParameterValues.get(paramName);
		if (defVal == null) {
			defVal = "";
			defaultParameterValues.put(paramName, defVal);
		}
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

	/**
	 * Construct a simple status response object.
	 *
	 * Status response indicates the server is busy carrying out the request and
	 * will have results later.
	 *
	 * @param code (string) error code
	 * @param msg the error message
	 * @param checkAgainMs advice for how long to wait before asking again (ms) (if 0, don't include this)
	 * @return the data object representing the error message
	 */
	public static DataObject statusObject(String code, String msg, int checkAgainMs) {
		DataObjectMapElement status = new DataObjectMapElement();
		status.put("code", new DataObjectString(code));
		status.put("message", new DataObjectString(msg));
		if (checkAgainMs != 0)
			status.put("check-again-ms", checkAgainMs);
		DataObjectMapElement rv = new DataObjectMapElement();
		rv.put("status", status);
		return rv;
	}

	public static TextPattern parsePatt(String pattern, String language) throws QueryException {
		return parsePatt(pattern, language, true);
	}

	public static TextPattern parsePatt(String pattern, String language, boolean required) throws QueryException {
		if (pattern == null || pattern.length() == 0) {
			if (required)
				throw new QueryException("NO_PATTERN_GIVEN", "Text search pattern required. Please specify 'patt' parameter.");
			return null; // not required, ok
		}

		if (language.equals("corpusql")) {
			try {
				return CorpusQueryLanguageParser.parse(pattern);
			} catch (ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in pattern: " + e.getMessage());
			} catch (TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR", "Syntax error in pattern: " + e.getMessage());
			}
		}

		// TODO: phrasequery?, contextql, luceneql, ...

		throw new QueryException("UNKNOWN_PATT_LANG", "Unknown pattern language '" + language + "'. Supported: corpusql, contextql, luceneql");
	}

	public static Filter parseFilter(String filter, String filterLang) throws QueryException {
		return parseFilter(filter, filterLang, false);
	}

	public static Filter parseFilter(String filter, String filterLang, boolean required) throws QueryException {
		if (filter == null || filter.length() == 0) {
			if (required)
				throw new QueryException("NO_FILTER_GIVEN", "Document filter required. Please specify 'filter' parameter.");
			return null; // not required
		}

		if (!filterLang.equals("luceneql"))
			throw new QueryException("UNKNOWN_FILTER_LANG", "Unknown filter language '" + filterLang + "'. Only 'luceneql' supported.");

		try {
			QueryParser parser = new QueryParser(Version.LUCENE_42, "", new BLDutchAnalyzer());
			Query query = parser.parse(filter);
			return new QueryWrapperFilter(query);
		} catch (org.apache.lucene.queryparser.classic.ParseException e) {
			throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing document filter query: " + e.getMessage());
		} catch (org.apache.lucene.queryparser.classic.TokenMgrError e) {
			throw new QueryException("FILTER_SYNTAX_ERROR", "Error parsing document filter query: " + e.getMessage());
		}
	}

	public int getDefaultCheckAgainAdviceMs() {
		return defaultCheckAgainAdviceMs;
	}

	public boolean isDebugMode() {
		return debugMode;
	}

	public void setDebugMode(boolean debugMode) {
		this.debugMode = debugMode;
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

}
