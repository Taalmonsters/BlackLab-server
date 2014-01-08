package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMap;

public class RequestHandlerDebug extends RequestHandler {

	public RequestHandlerDebug(HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		DataObjectMap response = new DataObjectMap();
		response.put("index-name", indexName);
		response.put("resource", urlResource);
		response.put("rest", urlPathInfo);
		response.put("query-string", request.getQueryString());
		return response;
	}

}
