package nl.inl.blacklab.server.requesthandlers;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.IndexStructure;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchManager;
import nl.inl.blacklab.server.search.SearchParameters;
import nl.inl.blacklab.server.search.SearchUtil;

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
		availableHandlers.put("debug", RequestHandlerDebug.class);
		availableHandlers.put("hits", RequestHandlerHits.class);
		availableHandlers.put("hits-grouped", RequestHandlerHitsGrouped.class);
		availableHandlers.put("docs", RequestHandlerDocs.class);
		availableHandlers.put("docs-grouped", RequestHandlerDocsGrouped.class);
		availableHandlers.put("doc-contents", RequestHandlerDocContents.class);
		availableHandlers.put("doc-snippet", RequestHandlerDocSnippet.class);
		availableHandlers.put("doc-info", RequestHandlerDocInfo.class);
		availableHandlers.put("cache-info", RequestHandlerCacheInfo.class);
		availableHandlers.put("", RequestHandlerIndexStructure.class);
	}

	/**
	 * Handle a request by dispatching it to the corresponding subclass.
	 *
	 * @param servlet the servlet object
	 * @param request the request object
	 * @param debugMode whether we're in debug mode
	 * @return the response data
	 */
	public static DataObject handle(BlackLabServer servlet, HttpServletRequest request, boolean debugMode) {

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

		logger.debug("IP: " + request.getRemoteAddr());

		// Choose the RequestHandler subclass
		RequestHandler requestHandler;
		if (indexName.equals("cache-info") && debugMode) {
			requestHandler = new RequestHandlerCacheInfo(servlet, request, indexName, urlResource, urlPathInfo);
		} else if (indexName.length() == 0) {
			// No index or operation given; server info
			requestHandler = new RequestHandlerServerInfo(servlet, request, indexName, urlResource, urlPathInfo);
		} else {
			// Choose based on urlResource
			try {
				String handlerName = urlResource;

				// HACK to avoid having a different url resource for
				// the lists of (hit|doc) groups: instantiate a different
				// request handler class in this case.
				if (debugMode && handlerName.length() > 0 && !handlerName.equals("hits") && !handlerName.equals("docs")) {
					handlerName = "debug";
				}
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
			}
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

	/** Our user id */
	String userId;

	RequestHandler(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathInfo) {
		this.servlet = servlet;
		this.request = request;
		searchMan = servlet.getSearchManager();
		searchParam = servlet.getSearchParameters(request, indexName);
		this.indexName = indexName;
		this.urlResource = urlResource;
		this.urlPathInfo = urlPathInfo;

		userId = request.getSession().getId();
		if (searchMan.mayOverrideUserId(request.getRemoteAddr()) && request.getParameter("userid") != null) {
			userId = request.getParameter("userid");
			logger.debug("userid overridden: " + userId);
		}

	}

	private void setDebug(boolean debugMode) {
		this.debugMode = debugMode;
	}

	public void debug(Logger logger, String msg) {
		logger.debug(shortUserId() + " " + msg);
	}

	public void warn(Logger logger, String msg) {
		logger.warn(shortUserId() + " " + msg);
	}

	public void info(Logger logger, String msg) {
		logger.info(shortUserId() + " " + msg);
	}

	public void error(Logger logger, String msg) {
		logger.error(shortUserId() + " " + msg);
	}

	/**
	 * Returns the complete request URL
	 *
	 * @return the complete request URL
	 */
	@SuppressWarnings("unused")
	private String getRequestUrl() {
		String pathInfo = request.getPathInfo();
		if (pathInfo == null)
			pathInfo = "";
		String queryString = request.getQueryString();
		if (queryString == null)
			queryString = "";
		else
			queryString = "?" + queryString;
		return request.getServletPath() + pathInfo + queryString;
	}

	/**
	 * Get the user id.
	 *
	 * Used for logging and making sure 1 user doesn't run too many queries.
	 *
	 * Right now, we simply use the session id, or the value sent in a parameter.
	 *
	 * @return the unique user id
	 */
	public String getUserId() {
		return userId;
	}

	/**
	 * Return the start of the user id.
	 *
	 * Useful for logging; should be enough for unique identification.
	 *
	 * @return the unique session id
	 */
	public String shortUserId() {
		return getUserId().substring(0, 6);
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
			debug(logger, "Illegal integer value for parameter '" + paramName + "': " + str);
			return false;
		}
	}

	/**
	 * Get document information (metadata, contents authorization)
	 *
	 * @param indexName name of the index
	 * @param struct the index structure
	 * @param document Lucene document
	 * @return the document information
	 */
	public DataObjectMapElement getDocumentInfo(String indexName, IndexStructure struct, Document document) {
		DataObjectMapElement docInfo = new DataObjectMapElement();
		for (String metadataFieldName: struct.getMetadataFields()) {
			String value = document.get(metadataFieldName);
			if (value != null)
				docInfo.put(metadataFieldName, value);
		}
		docInfo.put("may-view", searchMan.mayViewContents(indexName, document));
		return docInfo;
	}

}
