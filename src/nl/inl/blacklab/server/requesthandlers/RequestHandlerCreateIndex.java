package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.User;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerCreateIndex extends RequestHandler {
	public RequestHandlerCreateIndex(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws QueryException {
		if (indexName != null && indexName.length() > 0) {
			// Create index and return success
			try {
				searchMan.createIndex(indexName);
				
				DataObjectMapElement response = DataObject.statusObject("SUCCESS", "Index created succesfully.");
				//response.put("url", ServletUtil.getServletBaseUrl(request) + "/" + indexName);
				return response;
			} catch (QueryException e) {
				throw e;
			} catch (Exception e) {
				return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 11));
			}
		}
		
		return DataObject.errorObject("CANNOT_CREATE_INDEX", "Could not create index '" + indexName + "'. Specify a valid name.");
	}

}
