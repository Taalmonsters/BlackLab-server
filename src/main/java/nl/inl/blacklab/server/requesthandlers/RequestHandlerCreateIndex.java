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
		// Create index and return success
		try {
			String newIndexName = request.getParameter("name");
			if (newIndexName == null || newIndexName.length() == 0)
				return Response.badRequest("ILLEGAL_INDEX_NAME", "You didn't specify the required name parameter.");
			String displayName = request.getParameter("display");
			String documentFormat = request.getParameter("format");

			debug(logger, "REQ create index: " + newIndexName + ", " + displayName + ", " + documentFormat);
			if (!user.isLoggedIn() || !newIndexName.startsWith(user.getUserId() + ":")) {
				logger.debug("(forbidden, cannot create index in another user's area)");
				return Response.forbidden("You can only create indices in your own private area.");
			}

			searchMan.createIndex(newIndexName, displayName, documentFormat);

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
}
