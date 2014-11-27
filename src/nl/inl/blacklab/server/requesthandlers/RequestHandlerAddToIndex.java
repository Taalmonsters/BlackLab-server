package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.User;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerAddToIndex extends RequestHandler {
	public RequestHandlerAddToIndex(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		String status = searchMan.getIndexStatus(indexName);
		if (!status.equals("available"))
			return DataObject.errorObject("INDEX_UNAVAILABLE", "The index '" + indexName + "' is not available right now. Status: " + status);
		
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("result", "nothing");
		return response;
	}

}
