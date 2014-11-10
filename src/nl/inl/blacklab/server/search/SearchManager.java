package nl.inl.blacklab.server.search;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.queryParser.contextql.ContextualQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.CorpusQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.ParseException;
import nl.inl.blacklab.queryParser.corpusql.TokenMgrError;
import nl.inl.blacklab.queryParser.lucene.LuceneQueryParser;
import nl.inl.blacklab.search.CompleteQuery;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TextPattern;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.FileUtil;
import nl.inl.util.MemoryUtil;
import nl.inl.util.FileUtil.FileTask;
import nl.inl.util.json.JSONArray;
import nl.inl.util.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Version;

public class SearchManager {
	private static final int MAX_USER_INDICES = 10;

	private static final Logger logger = Logger.getLogger(SearchManager.class);

	private static final String ILLEGAL_NAME_ERROR = "Illegal index name (only letters, digits, underscores and dashes allowed): ";

	/**
	 * A file filter that returns readable directories only; used for scanning
	 * collections dirs
	 */
	private static FileFilter readableDirFilter = new FileFilter() {
		@Override
		public boolean accept(File f) {
			return f.isDirectory() && f.canRead();
		}
	};

	/**
	 * When the SearchManager was created. Used in logging to show ms since
	 * server start instead of all-time.
	 */
	long createdAt = System.currentTimeMillis();

	/**
	 * How long the server should wait for a quick answer when starting a
	 * nonblocking request. If the answer is found within this time, the client
	 * needs only one request even in nonblocking mode.
	 */
	private int waitTimeInNonblockingModeMs;

	/**
	 * The minimum time to advise a client to wait before checking the status of
	 * a search again.
	 */
	private int checkAgainAdviceMinimumMs;

	/**
	 * What number to divide the search time so far by to get the check again
	 * advice. E.g. if this is set to 5 (the default), if a search has been
	 * running for 10 seconds, clients are advised to wait 2 seconds before
	 * checking the status again.
	 */
	private int checkAgainAdviceDivider;

	/** Maximum context size allowed */
	private int maxContextSize;

	/** Maximum snippet size allowed */
	private int maxSnippetSize;

	/** Parameters involved in search */
	private List<String> searchParameterNames;

	/** Default values for request parameters */
	private Map<String, String> defaultParameterValues;

	// /** Run in debug mode or not? [no] */
	// private boolean debugMode;

	/** Default number of hits/results per page [20] */
	private int defaultPageSize;

	/** Maximum value allowed for number parameter */
	private int maxPageSize;

	/** Our current set of indices (with dir and mayViewContent setting) */
	private Map<String, IndexParam> indexParam;

	/**
	 * The status of each index, i.e. "available" or "indexing". If no status is
	 * stored here, the status is "available".
	 */
	Map<String, String> indexStatus;

	/** Configured index collections directories */
	private List<File> collectionsDirs;

	/**
	 * Logged-in users will have their own private collections dir. This is the
	 * parent of that dir.
	 */
	private File userCollectionsDir;

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

	/**
	 * Maximum number of simultaneously running jobs started by the same user.
	 * [20] Please note that a search may start 2-4 jobs, so don't set this too
	 * low. This is just meant to prevent over-eager scripts and other abuse.
	 * Regular users should never hit this limit.
	 */
	private long maxRunningJobsPerUser;

	/** IP addresses for which debug mode will be turned on. */
	private Set<String> debugModeIps;

	/** The default output type, JSON or XML. */
	private DataFormat defaultOutputType;

	/**
	 * Which IPs are allowed to override the userId using a parameter.
	 */
	private Set<String> overrideUserIdIps;

	/**
	 * How long the client may used a cached version of the results we give
	 * them. This is used to write HTTP cache headers. A value of an hour or so
	 * seems reasonable.
	 */
	private int clientCacheTimeSec;

	/** Maximum allowed value for maxretrieve parameter (-1 = no limit). */
	private int maxHitsToRetrieveAllowed;

	/** Maximum allowed value for maxcount parameter (-1 = no limit). */
	private int maxHitsToCountAllowed;

	// private JSONObject properties;

	public SearchManager(JSONObject properties) {
		logger.debug("SearchManager created");

		// this.properties = properties;
		if (properties.has("debugModeIps")) {
			JSONArray jsonDebugModeIps = properties
					.getJSONArray("debugModeIps");
			debugModeIps = new HashSet<String>();
			for (int i = 0; i < jsonDebugModeIps.length(); i++) {
				debugModeIps.add(jsonDebugModeIps.getString(i));
			}
		}

		// Request properties
		if (properties.has("requests")) {
			JSONObject reqProp = properties.getJSONObject("requests");
			defaultOutputType = DataFormat.XML; // XML if nothing specified
												// (because
												// of browser's default Accept
												// header)
			if (reqProp.has("defaultOutputType"))
				defaultOutputType = ServletUtil.getOutputTypeFromString(
						reqProp.getString("defaultOutputType"), DataFormat.XML);
			defaultPageSize = JsonUtil.getIntProp(reqProp, "defaultPageSize",
					20);
			maxPageSize = JsonUtil.getIntProp(reqProp, "maxPageSize", 1000);
			defaultPatternLanguage = JsonUtil.getProperty(reqProp,
					"defaultPatternLanguage", "corpusql");
			defaultFilterLanguage = JsonUtil.getProperty(reqProp,
					"defaultFilterLanguage", "luceneql");
			defaultBlockingMode = JsonUtil.getBooleanProp(reqProp,
					"defaultBlockingMode", true);
			defaultContextSize = JsonUtil.getIntProp(reqProp,
					"defaultContextSize", 5);
			maxContextSize = JsonUtil.getIntProp(reqProp, "maxContextSize", 20);
			maxSnippetSize = JsonUtil
					.getIntProp(reqProp, "maxSnippetSize", 100);
			maxContextSize = JsonUtil.getIntProp(reqProp, "maxContextSize", 20);
			Hits.setDefaultMaxHitsToRetrieve(JsonUtil.getIntProp(reqProp,
					"defaultMaxHitsToRetrieve",
					Hits.getDefaultMaxHitsToRetrieve()));
			Hits.setDefaultMaxHitsToCount(JsonUtil.getIntProp(reqProp,
					"defaultMaxHitsToCount", Hits.getDefaultMaxHitsToCount()));
			maxHitsToRetrieveAllowed = JsonUtil.getIntProp(reqProp,
					"maxHitsToRetrieveAllowed", 10000000);
			maxHitsToCountAllowed = JsonUtil.getIntProp(reqProp,
					"maxHitsToCountAllowed", -1);
			JSONArray jsonOverrideUserIdIps = reqProp
					.getJSONArray("overrideUserIdIps");
			overrideUserIdIps = new HashSet<String>();
			for (int i = 0; i < jsonOverrideUserIdIps.length(); i++) {
				overrideUserIdIps.add(jsonOverrideUserIdIps.getString(i));
			}
		}

		// Performance properties
		if (properties.has("performance")) {
			JSONObject perfProp = properties.getJSONObject("performance");
			minFreeMemForSearchMegs = JsonUtil.getIntProp(perfProp,
					"minFreeMemForSearchMegs", 50);
			maxRunningJobsPerUser = JsonUtil.getIntProp(perfProp,
					"maxRunningJobsPerUser", 20);
			checkAgainAdviceMinimumMs = JsonUtil.getIntProp(perfProp,
					"checkAgainAdviceMinimumMs", 200);
			checkAgainAdviceDivider = JsonUtil.getIntProp(perfProp,
					"checkAgainAdviceDivider", 5);
			waitTimeInNonblockingModeMs = JsonUtil.getIntProp(perfProp,
					"waitTimeInNonblockingModeMs", 100);
			clientCacheTimeSec = JsonUtil.getIntProp(perfProp,
					"clientCacheTimeSec", 3600);

			// Cache properties
			JSONObject cacheProp = perfProp.getJSONObject("cache");

			// Start with empty cache
			cache = new SearchCache(cacheProp);
		}

		// Find the indices
		indexParam = new HashMap<String, IndexParam>();
		indexStatus = new HashMap<String, String>();
		boolean indicesFound = false;
		if (properties.has("indices")) {
			JSONObject indicesMap = properties.getJSONObject("indices");
			Iterator<?> it = indicesMap.keys();
			while (it.hasNext()) {
				String indexName = (String) it.next();
				JSONObject indexConfig = indicesMap.getJSONObject(indexName);

				File dir = JsonUtil.getFileProp(indexConfig, "dir", null);
				if (dir == null || !dir.canRead()) {
					logger.error("Index directory for index '" + indexName
							+ "' does not exist or cannot be read: " + dir);
					continue;
				}

				String pid = JsonUtil.getProperty(indexConfig, "pid", "");
				if (pid.length() != 0) {
					// Should be specified in index metadata now, not in
					// blacklab-server.json.
					logger.error("blacklab-server.json specifies 'pid' property for index '"
							+ indexName
							+ "'; this setting should not be in blacklab-server.json but in the blacklab index metadata!");
				}

				// Does the settings file indicate whether or not contents may
				// be viewed?
				boolean mayViewContentsSet = indexConfig.has("mayViewContent");
				if (mayViewContentsSet) {
					// Yes; store the setting.
					boolean mayViewContent = indexConfig
							.getBoolean("mayViewContent");
					indexParam.put(indexName, new IndexParam(dir, pid,
							mayViewContent));
				} else {
					// No; record that we don't know (i.e. use the index
					// metadata setting).
					indexParam.put(indexName, new IndexParam(dir, pid));
				}

				indicesFound = true;
			}
		}

		// Collections
		collectionsDirs = new ArrayList<File>();
		if (properties.has("indexCollections")) {
			JSONArray indexCollectionsList = properties
					.getJSONArray("indexCollections");
			for (int i = 0; i < indexCollectionsList.length(); i++) {
				String strIndexCollection = indexCollectionsList.getString(i);
				File indexCollection = new File(strIndexCollection);
				if (indexCollection.canRead()) {
					indicesFound = true; // even if it contains none now, it
											// could in the future
					collectionsDirs.add(indexCollection);
				} else {
					logger.warn("Configured collection not found or not readable: "
							+ indexCollection);
				}
			}
		}

		// User collections dir
		if (properties.has("userCollectionsDir")) {
			userCollectionsDir = new File(
					properties.getString("userCollectionsDir"));
			if (!userCollectionsDir.canRead()) {
				logger.error("Configured user collections dir not found or not readable: "
						+ userCollectionsDir);
				userCollectionsDir = null;
			}
		}

		if (!indicesFound)
			throw new RuntimeException(
					"Configuration error: no indices or collections available. Put blacklab-server.json on classpath (i.e. Tomcat shared or lib dir) with at least: { \"indices\": { \"myindex\": { \"dir\": \"/path/to/my/index\" } } } ");

		// Keep a list of searchparameters.
		searchParameterNames = Arrays.asList("resultsType", "patt", "pattlang",
				"pattfield", "filter", "filterlang", "sort", "group",
				"viewgroup", "collator", "first", "number", "wordsaroundhit",
				"hitstart", "hitend", "facets", "waitfortotal",
				"includetokencount", "usecontent", "wordstart", "wordend",
				"calc", "maxretrieve", "maxcount", "property", "sensitive");

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
		defaultParameterValues.put("waitfortotal", "no");
		defaultParameterValues.put("includetokencount", "no");
		defaultParameterValues.put("wordsaroundhit", "" + defaultContextSize);
		defaultParameterValues.put("usecontent", "fi");
		defaultParameterValues.put("wordstart", "-1");
		defaultParameterValues.put("wordend", "-1");
		defaultParameterValues.put("calc", "");
		defaultParameterValues.put("maxretrieve",
				"" + Hits.getDefaultMaxHitsToRetrieve());
		defaultParameterValues.put("maxcount",
				"" + Hits.getDefaultMaxHitsToCount());
		defaultParameterValues.put("sensitive", "no");
		defaultParameterValues.put("property", "word");
	}

	/**
	 * Return the current user's collection dir.
	 * 
	 * @param userId
	 *            the current user
	 * 
	 * @return the user's collection dir, or null if none
	 */
	private File getUserCollectionDir(String userId) {
		if (userCollectionsDir == null)
			return null;
		File dir = new File(userCollectionsDir, userId);
		if (!dir.canRead())
			return null;
		return dir;
	}

	public List<String> getSearchParameterNames() {
		return searchParameterNames;
	}

	/**
	 * Find an index given its name.
	 * 
	 * Looks at explicitly configured indices as well as collections.
	 * 
	 * If a user is logged in, only looks in the user's private index
	 * collection.
	 * 
	 * @param name
	 *            the index name
	 * @param user
	 *            the user (userid if logged in, otherwise only session id)
	 * @return the index dir and mayViewContents setting
	 */
	private IndexParam getIndexParam(String name, User user) {
		if (user.isLoggedIn()) {
			// User is logged in; only look in user's private index collection.
			File dir = getUserCollectionDir(user.getUserId());
			return findIndexInCollection(name, dir, false);
		}

		// Already in the cache?
		if (indexParam.containsKey(name)) {
			IndexParam p = indexParam.get(name);

			// Check if it's still there.
			if (p.getDir().canRead())
				return p;

			// Directory isn't accessible any more; remove from cache
			indexParam.remove(name);
		}

		// Find it in a collection
		for (File collection : collectionsDirs) {
			IndexParam p = findIndexInCollection(name, collection, true);
			if (p != null)
				return p;
		}

		return null;
	}

	/**
	 * Search a collection for an index name.
	 * 
	 * Adds index parameters to the cache if found.
	 * 
	 * @param name
	 *            name of the index
	 * @param collection
	 *            the collection dir
	 * @param addToCache
	 *            if true, add parameters to the cache if found
	 * @return the index parameters if found.
	 */
	private IndexParam findIndexInCollection(String name, File collection,
			boolean addToCache) {
		// Look for the index in this collection dir
		File dir = new File(collection, name);
		if (dir.canRead() && Searcher.isIndex(dir)) {
			// Found it. Add to the cache and return
			IndexParam p = new IndexParam(dir);
			if (addToCache)
				indexParam.put(name, p);
			return p;
		}
		return null;
	}

	public static boolean isValidIndexName(String indexName) {
		return indexName.matches("[a-zA-Z0-9_\\-]+");
	}

	/**
	 * Get the Searcher object for the specified index.
	 * 
	 * @param indexName
	 *            the index we want to search
	 * @param user
	 *            user that wants to access the index
	 * @return the Searcher object for that index
	 * @throws IndexOpenException
	 *             if not found or open error
	 */
	@SuppressWarnings("deprecation")
	// for call to _setPidField() and _setContentViewable()
	public synchronized Searcher getSearcher(String indexName, User user)
			throws IndexOpenException {
		if (!isValidIndexName(indexName))
			throw new RuntimeException(ILLEGAL_NAME_ERROR + indexName);

		String prefixedName = getPrefixedIndexName(indexName, user);

		if (searchers.containsKey(prefixedName)) {
			Searcher searcher = searchers.get(prefixedName);
			if (searcher.getIndexDirectory().canRead())
				return searcher;
			// Index was (re)moved; remove Searcher from cache.
			searchers.remove(prefixedName);
			// Maybe we can find an index with this name elsewhere?
		}
		IndexParam par = getIndexParam(indexName, user);
		if (par == null) {
			throw new IndexOpenException("Index " + indexName + " not found");
		}
		File indexDir = par.getDir();
		Searcher searcher;
		try {
			logger.debug("Opening index '" + indexName + "', dir = " + indexDir);
			searcher = Searcher.open(indexDir);
		} catch (Exception e) {
			e.printStackTrace();
			throw new IndexOpenException("Could not open index '" + indexName
					+ "'", e);
		}
		searchers.put(prefixedName, searcher);

		// Figure out the pid from the index metadata and/or BLS config.
		String indexPid = searcher.getIndexStructure().pidField();
		if (indexPid == null)
			indexPid = "";
		String configPid = par.getPidField();
		if (indexPid.length() > 0 && !configPid.equals(indexPid)) {
			if (configPid.length() > 0) {
				logger.error("Different pid field configured in blacklab-server.json than in index metadata! ("
						+ configPid + "/" + indexPid + "); using the latter");
			}
			// Update index parameters with the pid field found in the metadata
			par.setPidField(indexPid);
		} else {
			// No pid configured in index, only in blacklab-server.json. We want
			// to get rid
			// of this (prints an error on startup), but it should still work
			// for now. Inject
			// the setting into the searcher.
			searcher.getIndexStructure()._setPidField(configPid);
		}
		if (indexPid.length() == 0 && configPid.length() == 0) {
			logger.warn("No pid given for index '" + indexName
					+ "'; using Lucene doc ids.");
		}

		// Look for the contentViewable setting in the index metadata
		boolean contentViewable = searcher.getIndexStructure()
				.contentViewable();
		boolean blsConfigContentViewable = par.mayViewContents();
		if (par.mayViewContentsSpecified()
				&& contentViewable != blsConfigContentViewable) {
			logger.error("Index metadata and blacklab-server.json configuration disagree on content view settings! Disallowing free content viewing.");
			par.setMayViewContent(false);
			searcher.getIndexStructure()._setContentViewable(false);
		}

		return searcher;
	}

	/**
	 * Does the specified index exist?
	 * 
	 * @param indexName
	 *            the index we want to check for
	 * @param user
	 *            user that wants to access the index
	 * @return true iff the index exists
	 * @throws QueryException
	 */
	public boolean indexExists(String indexName, User user)
			throws QueryException {
		if (!isValidIndexName(indexName))
			throw new RuntimeException(ILLEGAL_NAME_ERROR + indexName);
		IndexParam par = getIndexParam(indexName, user);
		if (par == null) {
			return false;
		}
		return Searcher.isIndex(par.getDir());
	}

	/**
	 * Create an empty user index.
	 * 
	 * Indices may only be created by a logged-in user in his own private area.
	 * The index name is strictly validated, disallowing any weird input.
	 * 
	 * @param indexName
	 *            the index name
	 * @param user
	 *            the logged-in user
	 * 
	 * @throws QueryException
	 *             if we're not allowed to create the index for whatever reason
	 * @throws IOException
	 *             if creation failed unexpectedly
	 */
	public void createIndex(String indexName, User user) throws QueryException,
			IOException {
		if (!user.isLoggedIn())
			throw new QueryException("CANNOT_CREATE_INDEX ",
					"Could not create index. Must be logged in.");
		if (!isValidIndexName(indexName))
			throw new QueryException("ILLEGAL_INDEX_NAME", ILLEGAL_NAME_ERROR
					+ indexName);
		if (indexExists(indexName, user))
			throw new QueryException("INDEX_ALREADY_EXISTS",
					"Could not create index. Index already exists.");
		int n = getAvailableIndices(user).size();
		if (n >= MAX_USER_INDICES)
			throw new QueryException("CANNOT_CREATE_INDEX ",
					"Could not create index. You already have the maximum of "
							+ n + " indices.");

		File userDir = getUserCollectionDir(user.getUserId());
		if (!userDir.canWrite())
			throw new QueryException("CANNOT_CREATE_INDEX ",
					"Could not create index. Cannot write in use dir.");

		File indexDir = new File(userDir, indexName);
		Searcher searcher = Searcher.createIndex(indexDir);
		searcher.close();
	}

	/**
	 * Delete a user index.
	 * 
	 * Only user indices are deletable. The owner must be logged in. The index
	 * name is strictly validated, disallowing any weird input. Many other
	 * checks are done to root out all kinds of special cases.
	 * 
	 * @param indexName
	 *            the index name
	 * @param user
	 *            the logged-in user
	 * 
	 * @throws QueryException
	 *             if we're not allowed to delete the index
	 */
	public void deleteUserIndex(String indexName, User user)
			throws QueryException {
		if (!user.isLoggedIn())
			throw new QueryException("CANNOT_DELETE_INDEX",
					"Could not delete index. Must be logged in.");
		if (!isValidIndexName(indexName))
			throw new QueryException("ILLEGAL_INDEX_NAME", ILLEGAL_NAME_ERROR
					+ indexName);
		if (!indexExists(indexName, user))
			throw new QueryException("CANNOT_OPEN_INDEX",
					"Could not open index '" + indexName
							+ "'. Please check the name.");
		File userDir = getUserCollectionDir(user.getUserId());
		File indexDir = new File(userDir, indexName);
		if (!indexDir.isDirectory())
			throw new QueryException("CANNOT_DELETE_INDEX ",
					"Could not delete index. Not an index.");
		if (!userDir.canWrite() || !indexDir.canWrite())
			throw new QueryException("CANNOT_DELETE_INDEX ",
					"Could not delete index. Check file permissions.");
		if (!indexDir.getParentFile().equals(userDir)) { // Yes, we're paranoid..
			throw new QueryException("CANNOT_DELETE_INDEX ",
					"Could not delete index. Not found in user dir.");
		}
		if (!Searcher.isIndex(indexDir)) { // ..but are we paranoid enough?
			throw new QueryException("CANNOT_DELETE_INDEX ",
					"Could not delete index. Not a BlackLab index.");
		}

		// Don't follow symlinks
		try {
			if (isSymlink(indexDir)) {
				throw new QueryException("CANNOT_DELETE_INDEX ", "Could not delete index. Is a symlink.");
			}
		} catch (IOException e1) {
			throw new QueryException("INTERNAL_ERROR", "An internal error occurred. Please contact the administrator. Error code: 13.");
		}

		// Can we even delete the whole tree? If not, don't even try.
		try {
			FileUtil.processTree(indexDir, new FileTask() {
				@Override
				public void process(File f) {
					if (!f.canWrite())
						throw new RuntimeException("Cannot delete " + f);
				}
			});
		} catch (Exception e) {
			throw new QueryException("CANNOT_DELETE_INDEX ",
					"Could not delete index. Can't delete all files/dirs.");
		}

		// Everything seems ok. Delete the index.
		delTree(indexDir);
	}

	// Copied from Apache Commons
	// (as allowed under the Apache License 2.0)
	public static boolean isSymlink(File file) throws IOException {
		if (file == null)
			throw new NullPointerException("File must not be null");
		File canon;
		if (file.getParent() == null) {
			canon = file;
		} else {
			File canonDir = file.getParentFile().getCanonicalFile();
			canon = new File(canonDir, file.getName());
		}
		return !canon.getCanonicalFile().equals(canon.getAbsoluteFile());
	}

	/**
	 * Delete an entire tree with files, subdirectories, etc.
	 * 
	 * CAREFUL, DANGEROUS!
	 *
	 * @param root
	 *            the directory tree to delete
	 */
	private static void delTree(File root) {
		if (!root.isDirectory())
			throw new RuntimeException("Not a directory: " + root);
		for (File f : root.listFiles()) {
			if (f.isDirectory())
				delTree(f);
			else
				f.delete();
		}
		root.delete();
	}

	private String getPrefixedIndexName(String indexName, User user) {
		String indexNameWithUserPrefix;
		if (user.isLoggedIn()) {
			indexNameWithUserPrefix = user.getUserId() + "/" + indexName;
		} else {
			indexNameWithUserPrefix = indexName;
		}
		return indexNameWithUserPrefix;
	}

	/**
	 * Get the Lucene Document id given the pid
	 * 
	 * @param searcher
	 *            our index
	 * @param pid
	 *            the pid string (or Lucene doc id if we don't use a pid)
	 * @return the document id, or -1 if it doesn't exist
	 * @throws IndexOpenException
	 */
	public static int getLuceneDocIdFromPid(Searcher searcher, String pid)
			throws IndexOpenException {
		String pidField = searcher.getIndexStructure().pidField(); // getIndexParam(indexName,
																	// user).getPidField();
		// Searcher searcher = getSearcher(indexName, user);
		if (pidField.length() == 0) {
			int luceneDocId;
			try {
				luceneDocId = Integer.parseInt(pid);
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException(
						"Pid must be a Lucene doc id, but it's not a number: "
								+ pid);
			}
			return luceneDocId;
		}
		boolean lowerCase = false; // HACK in case pid field is incorrectly
									// lowercased
		DocResults docResults;
		while (true) {
			String p = lowerCase ? pid.toLowerCase() : pid;
			TermQuery documentFilterQuery = new TermQuery(new Term(pidField, p));
			docResults = searcher.queryDocuments(documentFilterQuery);
			if (docResults.size() > 1) {
				// Should probably throw a fatal exception, but sometimes
				// documents
				// accidentally occur twice in a dataset...
				// TODO: make configurable whether or not a fatal exception is
				// thrown
				logger.error("Pid must uniquely identify a document, but it has "
						+ docResults.size() + " hits: " + pid);
			}
			if (docResults.size() == 0) {
				if (lowerCase)
					return -1; // tried with and without lowercasing; doesn't
								// exist
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
	 * 
	 * @param user
	 *            the current user (either userid for logged in user, or session
	 *            id otherwise)
	 * @return the list of index names
	 */
	public Collection<String> getAvailableIndices(User user) {

		if (user.isLoggedIn()) {
			File dir = getUserCollectionDir(user.getUserId());
			Set<String> indices = new HashSet<String>();
			if (dir != null) {
				for (File f : dir.listFiles(readableDirFilter)) {
					indices.add(f.getName());
				}
			}
			return indices;
		}

		// Scan collections for any new indices
		for (File dir : collectionsDirs) {
			addNewIndicesInCollection(dir);
		}

		// Remove indices that are no longer available
		List<String> remove = new ArrayList<String>();
		for (Map.Entry<String, IndexParam> e : indexParam.entrySet()) {
			if (!e.getValue().getDir().canRead()) {
				remove.add(e.getKey());
			}
		}
		for (String name : remove) {
			indexParam.remove(name);
		}

		return indexParam.keySet();
	}

	/**
	 * Scan a collection dir and add any new indices to our cache.
	 * 
	 * @param collectionDir
	 *            the collection directory
	 */
	private void addNewIndicesInCollection(File collectionDir) {
		for (File f : collectionDir.listFiles(readableDirFilter)) {
			if (!indexParam.containsKey(f.getName())) {
				// New one; add it
				indexParam.put(f.getName(), new IndexParam(f));
			}
		}
	}

	public JobWithHits searchHits(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "sort", "docpid",
				"maxretrieve", "maxcount");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobHitsSorted");
			return (JobHitsSorted) search(user, parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or
									// it's cached wrong
		parBasic.put("jobclass", "JobHits");
		return (JobHits) search(user, parBasic);
	}

	public JobWithDocs searchDocs(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "sort", "usecontent",
				"maxretrieve", "maxcount");
		String sort = parBasic.get("sort");
		if (sort != null && sort.length() > 0) {
			// Sorted hits
			parBasic.put("jobclass", "JobDocsSorted");
			return (JobDocsSorted) search(user, parBasic);
		}

		// No sort
		parBasic.remove("sort"); // unsorted must not include sort parameter, or
									// it's cached wrong
		parBasic.put("jobclass", "JobDocs");
		return (JobDocs) search(user, parBasic);
	}

	public JobHitsWindow searchHitsWindow(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "sort", "first", "number",
				"wordsaroundhit", "usecontent", "maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobHitsWindow");
		return (JobHitsWindow) search(user, parBasic);
	}

	public JobDocsWindow searchDocsWindow(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "sort", "first", "number",
				"wordsaroundhit", "usecontent", "maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobDocsWindow");
		return (JobDocsWindow) search(user, parBasic);
	}

	public JobHitsTotal searchHitsTotal(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobHitsTotal");
		return (JobHitsTotal) search(user, parBasic);
	}

	public JobDocsTotal searchDocsTotal(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobDocsTotal");
		return (JobDocsTotal) search(user, parBasic);
	}

	public JobHitsGrouped searchHitsGrouped(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "group", "sort",
				"maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobHitsGrouped");
		return (JobHitsGrouped) search(user, parBasic);
	}

	public JobDocsGrouped searchDocsGrouped(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("indexname", "patt",
				"pattlang", "filter", "filterlang", "group", "sort",
				"maxretrieve", "maxcount");
		parBasic.put("jobclass", "JobDocsGrouped");
		return (JobDocsGrouped) search(user, parBasic);
	}

	public JobFacets searchFacets(User user, SearchParameters par)
			throws IndexOpenException, QueryException, InterruptedException {
		SearchParameters parBasic = par.copyWithOnly("facets", "indexname",
				"patt", "pattlang", "filter", "filterlang");
		parBasic.put("jobclass", "JobFacets");
		return (JobFacets) search(user, parBasic);
	}

	/**
	 * Start a new search or return an existing Search object corresponding to
	 * these search parameters.
	 *
	 * @param user
	 *            user creating the job
	 * @param searchParameters
	 *            the search parameters
	 * @param blockUntilFinished
	 *            if true, wait until the search finishes; otherwise, return
	 *            immediately
	 * @return a Search object corresponding to these parameters
	 * @throws QueryException
	 *             if the query couldn't be executed
	 * @throws IndexOpenException
	 *             if the index couldn't be opened
	 * @throws InterruptedException
	 *             if the search thread was interrupted
	 */
	private Job search(User user, SearchParameters searchParameters)
			throws IndexOpenException, QueryException, InterruptedException {
		// Search the cache / running jobs for this search, create new if not
		// found.
		boolean performSearch = false;
		Job search;
		synchronized (this) {
			search = cache.get(searchParameters);
			if (search == null) {
				// Not found in cache

				// Do we have enough memory to start a new search?
				long freeMegs = MemoryUtil.getFree() / 1000000;
				if (freeMegs < minFreeMemForSearchMegs) {
					cache.removeOldSearches(); // try to free up space for next
												// search
					logger.warn("Can't start new search, not enough memory ("
							+ freeMegs + "M < " + minFreeMemForSearchMegs
							+ "M)");
					throw new QueryException("SERVER_BUSY",
							"The server is under heavy load right now. Please try again later.");
				}
				// logger.debug("Enough free memory: " + freeMegs + "M");

				// Is this user allowed to start another search?
				int numRunningJobs = 0;
				String uniqueId = user.uniqueId();
				Set<Job> runningJobs = runningJobsPerUser.get(uniqueId);
				Set<Job> newRunningJobs = new HashSet<Job>();
				if (runningJobs != null) {
					for (Job job : runningJobs) {
						if (!job.finished()) {
							numRunningJobs++;
							newRunningJobs.add(job);
						}
					}
				}
				if (numRunningJobs >= maxRunningJobsPerUser) {
					// User has too many running jobs. Can't start another one.
					runningJobsPerUser.put(uniqueId, newRunningJobs); // refresh
																		// the
																		// list
					logger.warn("Can't start new search, user already has "
							+ numRunningJobs + " jobs running.");
					throw new QueryException(
							"TOO_MANY_JOBS",
							"You already have too many running searches. Please wait for some previous searches to complete before starting new ones.");
				}

				// Create a new search object with these parameters and place it
				// in the cache
				search = Job.create(this, user, searchParameters);
				cache.put(search);

				// Update running jobs
				newRunningJobs.add(search);
				runningJobsPerUser.put(uniqueId, newRunningJobs);

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
		/*
		 * if (defVal == null) { defVal = "";
		 * defaultParameterValues.put(paramName, defVal); }
		 */
		return defVal;
	}

	public static boolean strToBool(String value)
			throws IllegalArgumentException {
		if (value.equals("true") || value.equals("1") || value.equals("yes")
				|| value.equals("on"))
			return true;
		if (value.equals("false") || value.equals("0") || value.equals("no")
				|| value.equals("off"))
			return false;
		throw new IllegalArgumentException("Cannot convert to boolean: "
				+ value);
	}

	public static int strToInt(String value) throws IllegalArgumentException {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Cannot convert to int: "
					+ value);
		}
	}

	/**
	 * Construct a simple error response object.
	 *
	 * @param code
	 *            (string) error code
	 * @param msg
	 *            the error message
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

	public TextPattern parsePatt(Searcher searcher, String pattern,
			String language) throws QueryException {
		return parsePatt(searcher, pattern, language, true);
	}

	public TextPattern parsePatt(Searcher searcher, String pattern,
			String language, boolean required) throws QueryException {
		if (pattern == null || pattern.length() == 0) {
			if (required)
				throw new QueryException("NO_PATTERN_GIVEN",
						"Text search pattern required. Please specify 'patt' parameter.");
			return null; // not required, ok
		}

		if (language.equals("corpusql")) {
			try {
				return CorpusQueryLanguageParser.parse(pattern);
			} catch (ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in CorpusQL pattern: " + e.getMessage());
			} catch (TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in CorpusQL pattern: " + e.getMessage());
			}
		} else if (language.equals("contextql")) {
			try {
				CompleteQuery q = ContextualQueryLanguageParser.parse(searcher,
						pattern);
				return q.getContentsQuery();
			} catch (nl.inl.blacklab.queryParser.contextql.TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in ContextQL pattern: " + e.getMessage());
			} catch (nl.inl.blacklab.queryParser.contextql.ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in ContextQL pattern: " + e.getMessage());
			}
		} else if (language.equals("luceneql")) {
			try {
				String field = searcher.getIndexStructure()
						.getMainContentsField().getName();
				LuceneQueryParser parser = new LuceneQueryParser(
						Version.LUCENE_42, field, searcher.getAnalyzer());
				return parser.parse(pattern);
			} catch (nl.inl.blacklab.queryParser.lucene.ParseException e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in LuceneQL pattern: " + e.getMessage());
			} catch (nl.inl.blacklab.queryParser.lucene.TokenMgrError e) {
				throw new QueryException("PATT_SYNTAX_ERROR",
						"Syntax error in LuceneQL pattern: " + e.getMessage());
			}
		}

		throw new QueryException("UNKNOWN_PATT_LANG",
				"Unknown pattern language '" + language
						+ "'. Supported: corpusql, contextql, luceneql.");
	}

	public static Query parseFilter(Searcher searcher, String filter,
			String filterLang) throws QueryException {
		return parseFilter(searcher, filter, filterLang, false);
	}

	public static Query parseFilter(Searcher searcher, String filter,
			String filterLang, boolean required) throws QueryException {
		if (filter == null || filter.length() == 0) {
			if (required)
				throw new QueryException("NO_FILTER_GIVEN",
						"Document filter required. Please specify 'filter' parameter.");
			return null; // not required
		}

		Analyzer analyzer = searcher.getAnalyzer();
		if (filterLang.equals("luceneql")) {
			try {
				QueryParser parser = new QueryParser(Version.LUCENE_42, "",
						analyzer);
				parser.setAllowLeadingWildcard(true);
				Query query = parser.parse(filter);
				return query;
			} catch (org.apache.lucene.queryparser.classic.ParseException e) {
				throw new QueryException("FILTER_SYNTAX_ERROR",
						"Error parsing LuceneQL filter query: "
								+ e.getMessage());
			} catch (org.apache.lucene.queryparser.classic.TokenMgrError e) {
				throw new QueryException("FILTER_SYNTAX_ERROR",
						"Error parsing LuceneQL filter query: "
								+ e.getMessage());
			}
		} else if (filterLang.equals("contextql")) {
			try {
				CompleteQuery q = ContextualQueryLanguageParser.parse(searcher,
						filter);
				return q.getFilterQuery();
			} catch (nl.inl.blacklab.queryParser.contextql.TokenMgrError e) {
				throw new QueryException("FILTER_SYNTAX_ERROR",
						"Error parsing ContextQL filter query: "
								+ e.getMessage());
			} catch (nl.inl.blacklab.queryParser.contextql.ParseException e) {
				throw new QueryException("FILTER_SYNTAX_ERROR",
						"Error parsing ContextQL filter query: "
								+ e.getMessage());
			}
		}

		throw new QueryException("UNKNOWN_FILTER_LANG",
				"Unknown filter language '" + filterLang
						+ "'. Supported: luceneql, contextql.");
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

	// public boolean mayViewContents(User user, String indexName, Document
	// document) {
	// IndexParam p = indexParam.get(indexName);
	// if (p == null)
	// return false;
	// return p.mayViewContents();
	// }

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
	 * 
	 * @param search
	 *            the search you want to check the status of
	 * @return how long you should wait before asking again
	 */
	public int getCheckAgainAdviceMs(Job search) {

		// Simple advice algorithm: the longer the search
		// has been running, the less frequently the client
		// should check its progress. Just divide the search time by
		// 5 with a configured minimum.
		int runningFor = search.ageInSeconds();
		int checkAgainAdvice = Math.min(checkAgainAdviceMinimumMs, runningFor
				* 1000 / checkAgainAdviceDivider);

		return checkAgainAdvice;
	}

	/**
	 * Get maximum allowed value for maxretrieve parameter.
	 * 
	 * @return the maximum, or -1 if there's no limit
	 */
	public int getMaxHitsToRetrieveAllowed() {
		return maxHitsToRetrieveAllowed;
	}

	/**
	 * Get maximum allowed value for maxcount parameter.
	 * 
	 * @return the maximum, or -1 if there's no limit
	 */
	public int getMaxHitsToCountAllowed() {
		return maxHitsToCountAllowed;
	}

	public int getMaxPageSize() {
		return maxPageSize;
	}

	public int getDefaultPageSize() {
		return defaultPageSize;
	}

	/**
	 * Check the current status of an index
	 * 
	 * @param indexName
	 *            the index
	 * @return the current status
	 */
	public String getIndexStatus(String indexName) {
		synchronized (indexStatus) {
			String status = indexStatus.get(indexName);
			if (status == null)
				status = "available";
			return status;
		}
	}

	/**
	 * Check if the index status is (still) the specified status, and if so,
	 * update the status to the new one.
	 * 
	 * To check if setting was succesful, see if the returned value equals the
	 * requested status.
	 * 
	 * @param indexName
	 *            the index to set the status for
	 * @param checkOldStatus
	 *            only set the new status if this is the current status
	 * @param status
	 *            the new status
	 * @return the resulting status of the index
	 */
	public String setIndexStatus(String indexName, String checkOldStatus,
			String status) {
		synchronized (indexStatus) {
			String oldStatus = getIndexStatus(indexName);
			if (!oldStatus.equals(checkOldStatus))
				return oldStatus;
			indexStatus.put(indexName, status);
			return status;
		}
	}

}
