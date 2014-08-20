package nl.inl.blacklab.server.requesthandlers;

import java.util.Collection;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * Get information about this BlackLab server.
 */
public class RequestHandlerServerInfo extends RequestHandler {

	public RequestHandlerServerInfo(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		debug(logger, "REQ serverinfo");

		Collection<String> indices = searchMan.getAvailableIndices();
		DataObjectList doIndices = new DataObjectList("index");
		//DataObjectMapAttribute doIndices = new DataObjectMapAttribute("index", "name");
		for (String indexName: indices) {
			//DataObjectMapElement doIndex = new DataObjectMapElement();
			//doIndex.put("pidField", searchMan.getIndexPidField(indexName));
			doIndices.add(indexName); //, doIndex);
		}

		DataObjectMapElement response = new DataObjectMapElement();
		response.put("blacklabBuildTime", Searcher.getBlackLabBuildTime());
		response.put("indices", doIndices);
		response.put("helpPageUrl", servlet.getServletContext().getContextPath() + "/help");
		if (debugMode) {
			response.put("cacheStatus", searchMan.getCacheStatusDataObject());
		}

		return response;
	}


}
