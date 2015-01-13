package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.search.User;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerCreateIndex extends RequestHandler {
	public RequestHandlerCreateIndex(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public Response handle() throws BlsException {
		if (indexName != null && indexName.length() > 0) {
			// Create index and return success
			try {
				searchMan.createIndex(indexName);
				
				return Response.status("SUCCESS", "Index created succesfully.", HttpServletResponse.SC_CREATED);
				//DataObjectMapElement response = DataObject.statusObject("SUCCESS", "Index created succesfully.");
				//response.put("url", ServletUtil.getServletBaseUrl(request) + "/" + indexName);
				//return new Response(response);
			} catch (BlsException e) {
				throw e;
			} catch (Exception e) {
				return Response.internalError(e, debugMode, 11);
			}
		}
		
		return Response.badRequest("CANNOT_CREATE_INDEX", "Could not create index '" + indexName + "'. Specify a valid name.");
	}

}
