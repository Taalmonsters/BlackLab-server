package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * Get debug info about the servlet and index.
 * Only available in debug mode (BlackLabServer.DEBUG_MODE == true)
 */
public class RequestHandlerDebug extends RequestHandler {

	public RequestHandlerDebug(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("index-name", indexName);
		response.put("resource", urlResource);
		response.put("rest", urlPathInfo);
		response.put("query-string", request.getQueryString());
		response.put("search-parameters", servlet.getSearchParameters(request, indexName).toString());
		return response;
	}

}
