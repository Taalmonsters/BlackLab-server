package nl.inl.blacklab.server;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMap;

public class RequestHandlerDebug extends RequestHandler {

	public RequestHandlerDebug(HttpServletRequest request) {
		super(request);
	}

	@Override
	public DataObject handle() {
		String servletPath = request.getServletPath();
		if (servletPath == null)
			servletPath = "";
		if (servletPath.startsWith("/"))
			servletPath = servletPath.substring(1);
		if (servletPath.endsWith("/"))
			servletPath = servletPath.substring(0, servletPath.length() - 1);
		String[] parts = servletPath.split("/", 3);
		String indexName = parts.length >= 1 ? parts[0] : "";
		String resource = parts.length >= 2 ? parts[1] : "";
		String rest = parts.length >= 3 ? parts[2] : "";

		DataObjectMap response = new DataObjectMap();
		response.put("servlet-path", servletPath);
		response.put("index-name", indexName);
		response.put("resource", resource);
		response.put("rest", rest);
		response.put("query-string", request.getQueryString());
		return response;
	}

}
