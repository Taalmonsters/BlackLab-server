package nl.inl.blacklab.server.requesthandlers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.QueryException;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerCreateIndex extends RequestHandler {
	public RequestHandlerCreateIndex(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws QueryException {
		//DataObjectMapElement response = new DataObjectMapElement();
		//response.put("result", "nothing");

		indexName = getStringParameter("indexName");
		if (indexName != null && indexName.length() > 0) {
			// Create index and return success
			try {
				searchMan.createIndex(indexName, user);
			} catch (IOException e) {
				return DataObject.errorObject("INTERNAL_ERROR", internalErrorMessage(e, debugMode, 11));
			}
			
			DataObjectMapElement response = new DataObjectMapElement();
			response.put("url", ServletUtil.getServletBaseUrl(request) + "/" + indexName);
			return response;
		}
		
		return DataObject.errorObject("CANNOT_CREATE_INDEX", "Could not create index '" + indexName + "'. Specify a valid name.");
	}

}
