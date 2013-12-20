package nl.inl.blacklab.server;

import java.util.LinkedHashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

public class RequestHandlerDebug extends RequestHandler {

	public RequestHandlerDebug(HttpServletRequest request) {
		super(request);
	}

	@Override
	public Response handle() {
		Map<String, String> response = new LinkedHashMap<String, String>(); // Linked to preserve insertion order
		String servletPath = request.getServletPath();
		if (servletPath == null)
			servletPath = "";
		response.put("servlet path", servletPath);
		if (servletPath.startsWith("/"))
			servletPath = servletPath.substring(1);
		if (servletPath.endsWith("/"))
			servletPath = servletPath.substring(0, servletPath.length() - 1);
		String[] parts = servletPath.split("/", 3);
		String indexName = parts.length >= 1 ? parts[0] : "";
		String resource = parts.length >= 2 ? parts[1] : "";
		String rest = parts.length >= 3 ? parts[2] : "";
		response.put("index name", indexName);
		response.put("resource", resource);
		response.put("rest", rest);
		response.put("query string", request.getQueryString());

		return new ResponseDebug(response);
	}

}
