package nl.inl.blacklab.server.requesthandlers;

import java.util.Collection;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.User;

/**
 * Get information about this BlackLab server.
 */
public class RequestHandlerServerInfo extends RequestHandler {

	public RequestHandlerServerInfo(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public Response handle() {
		Collection<String> indices = searchMan.getAllAvailableIndices(user.getUserId());
		DataObjectList doIndices = new DataObjectList("index");
		for (String indexName: indices) {
			doIndices.add(indexName); //, doIndex);
		}
		
		DataObjectMapElement doUser = new DataObjectMapElement();
		doUser.put("loggedIn", user.isLoggedIn());
		doUser.put("canCreateIndex", user.isLoggedIn() ? searchMan.canCreateIndex(user.getUserId()) : false);

		DataObjectMapElement response = new DataObjectMapElement();
		response.put("blacklabBuildTime", Searcher.getBlackLabBuildTime());
		response.put("indices", doIndices);
		response.put("user", doUser);
		response.put("helpPageUrl", servlet.getServletContext().getContextPath() + "/help");
		if (debugMode) {
			response.put("cacheStatus", searchMan.getCacheStatusDataObject());
		}

		Response responseObj = new Response(response);
		responseObj.setCacheAllowed(false); // You can create/delete indices, don't cache the list
		return responseObj;
	}


}
