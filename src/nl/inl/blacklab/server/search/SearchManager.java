package nl.inl.blacklab.server.search;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import nl.inl.blacklab.analysis.BLDutchAnalyzer;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.queryParser.corpusql.CorpusQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.ParseException;
import nl.inl.blacklab.queryParser.corpusql.TokenMgrError;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TextPattern;
import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.PropertiesUtil;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class SearchManager  {
	private static final Logger logger = Logger.getLogger(SearchManager.class);

	/** How long the server should wait for a quick answer when starting
	 * a nonblocking request. If the answer is found within this time,
	 * the client needs only one request even in nonblocking mode. */
	private int waitTimeInNonblockingModeMs;

	/** The default advice to give for when to check status of search again.
	 * TODO the longer a search has been running, the
	 *   higher the advised wait time should be.
	 */
	private int defaultCheckAgainAdviceMs;

	/** Maximum context size allowed */
	private int maxContextSize;

	/** Parameters involved in search */
	private List<String> searchParameterNames;

	/** Default values for request parameters */
	private Map<String, String> defaultParameterValues;

	/** Run in debug mode or not? */
	private boolean debugMode;

	/** Default number of hits/results per page */
	private int defaultPageSize;

	private Map<String, IndexParam> indexParam;

	/** The Searcher objects, one for each of the indices we can search. */
	private Map<String, Searcher> searchers = new HashMap<String, Searcher>();

	/** All running searches as well as recently run searches */
	private SearchCache cache;

	private String defaultPatternLanguage;

	private String defaultFilterLanguage;

	private boolean defaultBlockingMode;

	private int defaultContextSize;

	public SearchManager(Properties properties)  {
		logger.debug("SearchManager created");

		// TODO: interrupt long search (both automatically and manually)
		//   problem: searches depend on one another. we need reference counting or something to
		//            be able to cancel searches no-one is interested in anymore.

		// TODO: snel F5 achter elkaar drukken geeft "Search cache already contains different search object"

		debugMode = PropertiesUtil.getBooleanProp(properties, "debugMode", false);
		defaultPageSize = PropertiesUtil.getIntProp(properties, "defaultPageSize", 20);
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

		// Find the indices
		Enumeration<Object> keys = properties.keys();
		indexParam = new HashMap<String, IndexParam>();
		while (keys.hasMoreElements()) {
			String name = (String)keys.nextElement();
			if (name.startsWith("index.") && name.indexOf('.', 6) < 0) {
				String indexName = name.substring(6);
				File dir = PropertiesUtil.getFileProp(properties, name);
				if (!dir.exists()) {
					logger.error("Index directory for index '" + indexName + "' does not exist: " + dir);
					continue;
				}

				String pid = properties.getProperty("index." + indexName + ".pid", "");
				if (pid.length() == 0) {
					logger.warn("No pid given for index '" + indexName + "'; using Lucene doc ids.");
				}

				boolean mayViewContent = PropertiesUtil.getBooleanProp(properties, "index." + indexName + ".may-view-content", false);

				indexParam.put(indexName, new IndexParam(dir, pid, mayViewContent));
			}
		}
		if (indexParam.size() == 0)
			throw new RuntimeException("Configuration error: no indices available. Specify indexNames (space-separated) and indexDir_<name> for each index!");

		// Keep a list of searchparameters.
		searchParameterNames = Arrays.asList(
				"resultsType", "patt", "pattlang", "pattfield", "filter", "filterlang",
				"sort", "group", "viewgroup", "collator", "first", "number", "wordsaroundhit");

		// Set up the parameter default values
		defaultParameterValues = new HashMap<String, String>();
		defaultParameterValues.put("filterlang", defaultFilterLanguage);
		defaultParameterValues.put("pattlang", defaultPatternLanguage);
		defaultParameterValues.put("sort", "");
		defaultParameterValues.put("group", "");
		defaultParameterValues.put("viewgroup", "");
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
		return p.getDir();
	}

	/** Get the persistent identifier field for an index.
	 *
	 * @param indexName the index
	 * @return the persistent identifier field, or empty if none (use Lucene doc id)
	 */
	public String getIndexPidField(String indexName) {
		IndexParam p = indexParam.get(indexName);
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
	 * Get the Lucene Document given the pid
	 * @param indexName index name
	 * @param pid the pid string (or Lucene doc id if we don't use a pid)
	 * @return the document
	 * @throws IndexOpenException
	 */
	public Document getDocumentFromPid(String indexName, String pid) throws IndexOpenException {
		String pidField = getIndexPidField(indexName);
		Searcher searcher = getSearcher(indexName);
		if (pidField.length() == 0) {
			int luceneDocId;
			try {
				luceneDocId = Integer.parseInt(pid);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Pid must be a Lucene doc id, but it's not a number: " + pid);
			}
			return searcher.document(luceneDocId);
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
					return null; // tried with and without lowercasing; doesn't exist
				lowerCase = true; // try lowercase now
			} else {
				// size == 1, found!
				break;
			}
		}
		return docResults.get(0).getDocument();
	}

	/**
	 * Return the list of indices available for searching.
	 * @return the list of index names
	 */
	public Collection<String> getAvailableIndices() {
		return indexParam.keySet();
	}

	public JobWithHits searchHits(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobHitsSorted");
			return (JobHitsSorted)search(parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or it's cached wrong
		parBasic.put("jobclass", "JobHits");
		return (JobHits)search(parBasic);
	}

	public JobWithDocs searchDocs(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobDocsSorted");
			return (JobDocsSorted)search(parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or it's cached wrong
		parBasic.put("jobclass", "JobDocs");
		return (JobDocs)search(parBasic);
	}

	public JobHitsWindow searchHitsWindow(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort", "first", "number", "wordsaroundhit");
		parBasic.put("jobclass", "JobHitsWindow");
		return (JobHitsWindow)search(parBasic);
	}

	public JobDocsWindow searchDocsWindow(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "sort", "first", "number", "wordsaroundhit");
		parBasic.put("jobclass", "JobDocsWindow");
		return (JobDocsWindow)search(parBasic);
	}

	public JobHitsTotal searchHitsTotal(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobHitsTotal");
		return (JobHitsTotal)search(parBasic);
	}

	public JobDocsTotal searchDocsTotal(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobDocsTotal");
		return (JobDocsTotal)search(parBasic);
	}

	public JobHitsGrouped searchHitsGrouped(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "group", "sort");
		parBasic.put("jobclass", "JobHitsGrouped");
		return (JobHitsGrouped)search(parBasic);
	}

	public JobDocsGrouped searchDocsGrouped(SearchParameters par) throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt", "pattlang", "filter", "filterlang", "group", "sort");
		parBasic.put("jobclass", "JobDocsGrouped");
		return (JobDocsGrouped)search(parBasic);
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
	private Job search(SearchParameters searchParameters) throws IndexOpenException, QueryException, InterruptedException {
		// Search the cache / running jobs for this search, create new if not found.
		boolean performSearch = false;
		Job search;
		synchronized(this) {
			search = cache.get(searchParameters);
			if (search == null) {
				// Not found; create a new search object with these parameters and place it in the cache
				search = Job.create(this, searchParameters);
				cache.put(search);
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

	public boolean mayViewContents(String indexName, Document document) {
		IndexParam p = indexParam.get(indexName);
		return p.mayViewContents();
	}

	public DataFormat getContentsFormat(String indexName) {
		return DataFormat.XML; // could be made configurable
	}

	public int getMaxContextSize() {
		return maxContextSize;
	}


}
