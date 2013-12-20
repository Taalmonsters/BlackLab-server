package nl.inl.blacklab.server;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMap;
import nl.inl.blacklab.server.dataobject.DataObjectString;
import nl.inl.util.ExUtil;

/**
 * Handles a request. Requests are dispatched to Action* classes.
 */
public abstract class RequestHandler {
	//private static final Logger logger = Logger.getLogger(RequestHandler.class);

	/** The available request handlers by name */
	static Map<String, Class<? extends RequestHandler>> availableHandlers;

	static {
		availableHandlers = new HashMap<String, Class<? extends RequestHandler>>();
		availableHandlers.put("debug", RequestHandlerDebug.class);
	}

	public static DataObject handle(HttpServletRequest request) {
		String pathPart = "debug";
		try {
			Class<? extends RequestHandler> handlerClass = availableHandlers.get(pathPart);
			Constructor<? extends RequestHandler> ctor = handlerClass.getConstructor(HttpServletRequest.class);
			RequestHandler requestHandler = ctor.newInstance(request);
			return requestHandler.handle();
		} catch (NoSuchMethodException e) {
			return errorResponse("No handler for request type " + pathPart);
		} catch (Exception e) {
			throw ExUtil.wrapRuntimeException(e);
		}
	}

	private static DataObject errorResponse(String msg) {
		DataObjectMap rv = new DataObjectMap();
		rv.put("errorMessage", new DataObjectString(msg));
		return rv;
	}

	/** The HTTP request object */
	HttpServletRequest request;

	RequestHandler(HttpServletRequest request) {
		this.request = request;
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
