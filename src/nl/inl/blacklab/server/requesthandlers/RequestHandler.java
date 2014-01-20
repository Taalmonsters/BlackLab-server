package nl.inl.blacklab.server.requesthandlers;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMap;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.ExUtil;

/**
 * Handles a request. Requests are dispatched to Action* classes.
 */
public abstract class RequestHandler {
	//private static final Logger logger = Logger.getLogger(RequestHandler.class);

	/** Are we debugging? */
	private static boolean DEBUG = true;

	/** The available request handlers by name */
	static Map<String, Class<? extends RequestHandler>> availableHandlers;

	static {
		availableHandlers = new HashMap<String, Class<? extends RequestHandler>>();
		availableHandlers.put("debug", RequestHandlerDebug.class);
		availableHandlers.put("hitset", RequestHandlerHitset.class);
	}

	public static DataObject handle(BlackLabServer servlet, HttpServletRequest request) {

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

		try {
			String handlerName = urlResource;
			if (handlerName.equals("error") && DEBUG)
				return errorResponse("Testing error system");
			if (!availableHandlers.containsKey(handlerName))
				handlerName = "debug";
			if (handlerName.equals("debug") && !DEBUG)
				return errorResponse("Unknown operation");
			Class<? extends RequestHandler> handlerClass = availableHandlers.get(handlerName);
			Constructor<? extends RequestHandler> ctor = handlerClass.getConstructor(HttpServletRequest.class, String.class, String.class, String.class);
			RequestHandler requestHandler = ctor.newInstance(request, indexName, urlResource, urlPathInfo);
			requestHandler.setServlet(servlet);
			return requestHandler.handle();
		} catch (NoSuchMethodException e) {
			return errorResponse("No handler for resource " + urlResource);
		} catch (Exception e) {
			throw ExUtil.wrapRuntimeException(e);
		}
	}

	/**
	 * A simple error response object
	 *
	 * @param msg the error message
	 * @return the data object representing the error message
	 */
	static DataObject errorResponse(String msg) {
		DataObjectMap rv = new DataObjectMap();
		rv.put("error-message", new DataObjectString(msg));
		return rv;
	}

	/** The servlet object */
	BlackLabServer servlet;

	/** The HTTP request object */
	HttpServletRequest request;

	/** The BlackLab index we want to access, e.g. "opensonar" for "/opensonar/doc/1/content" */
	String indexName;

	/** The type of REST resource we're accessing, e.g. "doc" for "/opensonar/doc/1/content" */
	String urlResource;

	/** The part of the URL path after the resource name, e.g. "1/content" for "/opensonar/doc/1/content" */
	String urlPathInfo;

	RequestHandler(HttpServletRequest request, String indexName, String urlResource, String urlPathInfo) {
		this.request = request;
		this.indexName = indexName;
		this.urlResource = urlResource;
		this.urlPathInfo = urlPathInfo;
	}

	public void setServlet(BlackLabServer servlet) {
		this.servlet = servlet;
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
	 * Child classes should override this to handle the request.
	 * @return the response object
	 * @throws IOException
	 */
	public abstract DataObject handle() throws IOException;

}
