package nl.inl.blacklab.server.requesthandlers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.perdocument.DocCount;
import nl.inl.blacklab.perdocument.DocCounts;
import nl.inl.blacklab.perdocument.DocGroupProperty;
import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocPropertyMultiple;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchManager;
import nl.inl.blacklab.server.search.SearchParameters;
import nl.inl.blacklab.server.search.SearchUtil;
import nl.inl.blacklab.server.search.User;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Base class for request handlers, to handle the different types of
 * requests. The static handle() method will dispatch the request to the
 * appropriate subclass.
 */
public abstract class RequestHandler {
	static final Logger logger = Logger.getLogger(RequestHandler.class);

	/** The available request handlers by name */
	static Map<String, Class<? extends RequestHandler>> availableHandlers;

	// Fill the map with all the handler classes
	static {
		availableHandlers = new HashMap<String, Class<? extends RequestHandler>>();
		//availableHandlers.put("cache-info", RequestHandlerCacheInfo.class);
		availableHandlers.put("debug", RequestHandlerDebug.class);
		availableHandlers.put("docs", RequestHandlerDocs.class);
		availableHandlers.put("docs-grouped", RequestHandlerDocsGrouped.class);
		availableHandlers.put("doc-contents", RequestHandlerDocContents.class);
		availableHandlers.put("doc-snippet", RequestHandlerDocSnippet.class);
		availableHandlers.put("doc-info", RequestHandlerDocInfo.class);
		availableHandlers.put("fields", RequestHandlerFieldInfo.class);
		//availableHandlers.put("help", RequestHandlerBlsHelp.class);
		availableHandlers.put("hits", RequestHandlerHits.class);
		availableHandlers.put("hits-grouped", RequestHandlerHitsGrouped.class);
		availableHandlers.put("status", RequestHandlerIndexStatus.class);
		availableHandlers.put("termfreq", RequestHandlerTermFreq.class);
		availableHandlers.put("", RequestHandlerIndexStructure.class);
	}

	/**
	 * Handle a request by dispatching it to the corresponding subclass.
	 *
	 * @param servlet the servlet object
	 * @param request the request object
	 * @return the response data
	 */
	public static DataObject handle(BlackLabServer servlet, HttpServletRequest request) {
		boolean debugMode = servlet.getSearchManager().isDebugMode(request.getRemoteAddr());

		// Parse the URL
		String servletPath = request.getServletPath();
		if (servletPath == null)
			servletPath = "";
		if (servletPath.startsWith("/"))
			servletPath = servletPath.substring(1);
		if (servletPath.endsWith("/"))
			servletPath = servletPath.substring(0, servletPath.length() - 1);
		String[] parts = servletPath.split("/", 3);
		String indexName = parts.length >= 1 ? parts[0] : "";
		String urlResource = parts.length >= 2 ? parts[1] : "";
		String urlPathInfo = parts.length >= 3 ? parts[2] : "";
		
		// Choose the RequestHandler subclass
		RequestHandler requestHandler;
		
		String method = request.getMethod();
		if (method.equals("DELETE")) {
			// Index given and nothing else?
			if (indexName.length() == 0 || urlResource.length() > 0 || urlPathInfo.length() > 0) {
				return DataObject.errorObject("ILLEGAL_DELETE_REQUEST", "Illegal DELETE request.");
			}
			requestHandler = new RequestHandlerDeleteIndex(servlet, request, indexName, null, null);
		} else if (method.equals("POST")) {
			if (indexName.length() == 0) {
				// POST to /blacklab-server/ : create new index
				requestHandler = new RequestHandlerCreateIndex(servlet, request, indexName, urlResource, urlPathInfo);
			} else if (urlResource.equals("docs")) {
				if (!SearchManager.isValidIndexName(indexName))
					return DataObject.errorObject("ILLEGAL_INDEX_NAME", "Illegal index name (only word characters, underscore and dash allowed): " + indexName);
				
				// POST to /blacklab-server/indexName/docs/ : add data to index
				requestHandler = new RequestHandlerAddToIndex(servlet, request, indexName, urlResource, urlPathInfo);
			} else {
				return DataObject.errorObject("ILLEGAL_POST_REQUEST", "Cannot service this POST request. All retrieval must be done using GET.");
			}
		} else if (method.equals("GET")) {
			if (indexName.equals("cache-info") && debugMode) {
				requestHandler = new RequestHandlerCacheInfo(servlet, request, indexName, urlResource, urlPathInfo);
			} else if (indexName.equals("help")) {
				requestHandler = new RequestHandlerBlsHelp(servlet, request, indexName, urlResource, urlPathInfo);
			} else if (indexName.length() == 0) {
				// No index or operation given; server info
				requestHandler = new RequestHandlerServerInfo(servlet, request, indexName, urlResource, urlPathInfo);
			} else {
				// Choose based on urlResource
				try {
					String handlerName = urlResource;
	
					SearchManager searchManager = servlet.getSearchManager();
					String status = searchManager.getIndexStatus(indexName);
					if (!status.equals("available") && handlerName.length() > 0 && !handlerName.equals("debug") && !handlerName.equals("fields") && !handlerName.equals("status")) {
						return DataObject.errorObject("INDEX_UNAVAILABLE", "The index '" + indexName + "' is not available right now. Status: " + status);
					}
					
					if (debugMode && handlerName.length() > 0 && !handlerName.equals("hits") && !handlerName.equals("docs") && !handlerName.equals("fields") && !handlerName.equals("termfreq") && !handlerName.equals("status")) {
						handlerName = "debug";
					}
					// HACK to avoid having a different url resource for
					// the lists of (hit|doc) groups: instantiate a different
					// request handler class in this case.
					else if (handlerName.equals("docs") && urlPathInfo.length() > 0) {
						handlerName = "doc-info";
						String p = urlPathInfo;
						if (p.endsWith("/"))
							p = p.substring(0, p.length() - 1);
						if (urlPathInfo.endsWith("/contents")) {
							handlerName = "doc-contents";
						} else if (urlPathInfo.endsWith("/snippet")) {
							handlerName = "doc-snippet";
						}
					}
					else if (handlerName.equals("hits") || handlerName.equals("docs")) {
						if (request.getParameter("group") != null) {
							String viewgroup = request.getParameter("viewgroup");
							if (viewgroup == null || viewgroup.length() == 0)
								handlerName += "-grouped"; // list of groups instead of contents
						}
					}
	
					if (!availableHandlers.containsKey(handlerName))
						return DataObject.errorObject("UNKNOWN_OPERATION", "Unknown operation. Check your URL.");
					Class<? extends RequestHandler> handlerClass = availableHandlers.get(handlerName);
					Constructor<? extends RequestHandler> ctor = handlerClass.getConstructor(BlackLabServer.class, HttpServletRequest.class, String.class, String.class, String.class);
					//servlet.getSearchManager().getSearcher(indexName); // make sure it's open
					requestHandler = ctor.newInstance(servlet, request, indexName, urlResource, urlPathInfo);
				} catch (NoSuchMethodException e) {
					// (can only happen if the required constructor is not available in the RequestHandler subclass)
					logger.error("Could not get constructor to create request handler", e);
					return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 2));
				} catch (IllegalArgumentException e) {
					logger.error("Could not create request handler", e);
					return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 3));
				} catch (InstantiationException e) {
					logger.error("Could not create request handler", e);
					return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 4));
				} catch (IllegalAccessException e) {
					logger.error("Could not create request handler", e);
					return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 5));
				} catch (InvocationTargetException e) {
					logger.error("Could not create request handler", e);
					return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 6));
				}/* catch (IndexOpenException e) {
					return DataObject.errorObject("CANNOT_OPEN_INDEX", "Could not open index '" + indexName + "'. Please check the name.");
				}*/
			}
		} else {
			return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(new RuntimeException("RequestHandler.doGetPost called with wrong method: " + method), debugMode, 10));
		}
		if (debugMode)
			requestHandler.setDebug(debugMode);

		// Handle the request
		try {
			return requestHandler.handle();
		} catch (IndexOpenException e) {
			return DataObject.errorObject("CANNOT_OPEN_INDEX", debugMode ? e.getMessage() : "Could not open index '" + indexName + "'. Please check the name.");
		} catch (QueryException e) {
			return DataObject.errorObject(e.getErrorCode(), e.getMessage());
		} catch (InterruptedException e) {
			return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 7));
		}
	}

	public static String internalErrorMessage(Exception e, boolean debugMode, int code) {
		if (debugMode) {
			return e.getClass().getName() + ": " + e.getMessage() + " (Internal error code " + code + ")";
		}
		return "An internal error occurred. Please contact the administrator.  Error code: " + code + ".";
	}

	boolean debugMode;

	/** The servlet object */
	BlackLabServer servlet;

	/** The HTTP request object */
	HttpServletRequest request;

	/** Search parameters from request */
	SearchParameters searchParam;

	/** The BlackLab index we want to access, e.g. "opensonar" for "/opensonar/doc/1/content" */
	String indexName;

	/** The type of REST resource we're accessing, e.g. "doc" for "/opensonar/doc/1/content" */
	String urlResource;

	/** The part of the URL path after the resource name, e.g. "1/content" for "/opensonar/doc/1/content" */
	String urlPathInfo;

	/** The search manager, which executes and caches our searches */
	SearchManager searchMan;

	/** User id (if logged in) and/or session id */
	User user;

	RequestHandler(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathInfo) {
		this.servlet = servlet;
		this.request = request;
		searchMan = servlet.getSearchManager();
		searchParam = servlet.getSearchParameters(request, indexName);
		this.indexName = indexName;
		this.urlResource = urlResource;
		this.urlPathInfo = urlPathInfo;

		String sessionId = request.getSession().getId();
		String userId = null;  // TODO: determine user id
		if (searchMan.mayOverrideUserId(request.getRemoteAddr()) && request.getParameter("userid") != null) {
			userId = request.getParameter("userid");
			logger.debug("userid overridden: " + userId);
		}
		if (userId != null && userId.length() == 0)
			userId = null;
		user = new User(userId, sessionId);
		
		logger.info(ServletUtil.shortenIpv6(request.getRemoteAddr()) + " " + user.uniqueIdShort() + " " + request.getMethod() + " " + ServletUtil.getPathAndQueryString(request));
	}

	private void setDebug(boolean debugMode) {
		this.debugMode = debugMode;
	}

	public void debug(Logger logger, String msg) {
		logger.debug(user.uniqueIdShort() + " " + msg);
	}

	public void warn(Logger logger, String msg) {
		logger.warn(user.uniqueIdShort() + " " + msg);
	}

	public void info(Logger logger, String msg) {
		logger.info(user.uniqueIdShort() + " " + msg);
	}

	public void error(Logger logger, String msg) {
		logger.error(user.uniqueIdShort() + " " + msg);
	}

	/**
	 * Child classes should override this to handle the request.
	 * @return the response object
	 * @throws IndexOpenException if the index can't be opened
	 * @throws QueryException if the query can't be executed
	 * @throws InterruptedException if the thread was interrupted
	 */
	public abstract DataObject handle() throws IndexOpenException, QueryException, InterruptedException;

	/**
	 * Get a string parameter.
	 *
	 * Illegal values will return 0 and log a debug message.
	 *
	 * If the parameter was not specified, the default value will be used.
	 *
	 * @param paramName parameter name
	 * @return the integer value
	 */
	public String getStringParameter(String paramName) {
		return ServletUtil.getParameter(request, paramName, servlet.getSearchManager().getParameterDefaultValue(paramName));
	}

	/**
	 * Get an integer parameter.
	 *
	 * Illegal values will return 0 and log a debug message.
	 *
	 * If the parameter was not specified, the default value will be used.
	 *
	 * @param paramName parameter name
	 * @return the integer value
	 */
	public int getIntParameter(String paramName) {
		String str = getStringParameter(paramName);
		try {
			return SearchUtil.strToInt(str);
		} catch (IllegalArgumentException e) {
			debug(logger, "Illegal integer value for parameter '" + paramName + "': " + str);
			return 0;
		}
	}

	/**
	 * Get a boolean parameter.
	 *
	 * Valid values are: true, false, 1, 0, yes, no, on, off.
	 *
	 * Other values will return false and log a debug message.
	 *
	 * If the parameter was not specified, the default value will be used.
	 *
	 * @param paramName parameter name
	 * @return the boolean value
	 */
	protected boolean getBoolParameter(String paramName) {
		String str = getStringParameter(paramName).toLowerCase();
		try {
			return SearchUtil.strToBool(str);
		} catch (IllegalArgumentException e) {
			debug(logger, "Illegal boolean value for parameter '" + paramName + "': " + str);
			return false;
		}
	}

	/**
	 * Get document information (metadata, contents authorization)
	 *
	 * @param searcher our index
	 * @param document Lucene document
	 * @return the document information
	 */
	public DataObjectMapElement getDocumentInfo(Searcher searcher, Document document) {
		DataObjectMapElement docInfo = new DataObjectMapElement();
		IndexStructure struct = searcher.getIndexStructure();
		for (String metadataFieldName: struct.getMetadataFields()) {
			String value = document.get(metadataFieldName);
			if (value != null)
				docInfo.put(metadataFieldName, value);
		}
		String tokenLengthField = struct.getMainContentsField().getTokenLengthField();
		if (tokenLengthField != null)
			docInfo.put("lengthInTokens", document.get(tokenLengthField));
		docInfo.put("mayView", struct.contentViewable());
		return docInfo;
	}

	protected DataObjectMapAttribute getFacets(DocResults docsToFacet, String facetSpec) {
		DataObjectMapAttribute doFacets;
		DocProperty propMultipleFacets = DocProperty.deserialize(facetSpec);
		List<DocProperty> props = new ArrayList<DocProperty>();
		if (propMultipleFacets instanceof DocPropertyMultiple) {
			// Multiple facets requested
			for (DocProperty prop: (DocPropertyMultiple)propMultipleFacets) {
				props.add(prop);
			}
		} else {
			// Just a single facet requested
			props.add(propMultipleFacets);
		}
	
		doFacets = new DataObjectMapAttribute("facet", "name");
		for (DocProperty facetBy: props) {
			DocCounts facetCounts = docsToFacet.countBy(facetBy);
			facetCounts.sort(DocGroupProperty.size());
			DataObjectList doFacet = new DataObjectList("item");
			int n = 0, maxFacetValues = 10;
			int totalSize = 0;
			for (DocCount count: facetCounts) {
				DataObjectMapElement doItem = new DataObjectMapElement();
				doItem.put("value", count.getIdentity().toString());
				doItem.put("size", count.size());
				doFacet.add(doItem);
				totalSize += count.size();
				n++;
				if (n >= maxFacetValues)
					break;
			}
			if (totalSize < facetCounts.getTotalResults()) {
				DataObjectMapElement doItem = new DataObjectMapElement();
				doItem.put("value", "[REST]");
				doItem.put("size", facetCounts.getTotalResults() - totalSize);
				doFacet.add(doItem);
			}
			doFacets.put(facetBy.getName(), doFacet);
		}
		return doFacets;
	}

	protected Searcher getSearcher() throws IndexOpenException {
		return searchMan.getSearcher(indexName, user);
	}

	/**
	 * Get the pid for the specified document
	 * 
	 * @param searcher where we got this document from
	 * @param luceneDocId
	 *            Lucene document id
	 * @param document
	 *            the document object
	 * @return the pid string (or Lucene doc id in string form if index has no
	 *         pid field)
	 */
	public static String getDocumentPid(Searcher searcher, int luceneDocId,
			Document document) {
		String pidField = searcher.getIndexStructure().pidField(); //getIndexParam(indexName, user).getPidField();
		if (pidField.length() == 0)
			return "" + luceneDocId;
		return document.get(pidField);
	}

}
