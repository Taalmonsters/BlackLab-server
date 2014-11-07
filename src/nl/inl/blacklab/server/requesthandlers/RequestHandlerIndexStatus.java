package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerIndexStatus extends RequestHandler {
	
	public RequestHandlerIndexStatus(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException {
		//Searcher searcher = getSearcher();
		//IndexStructure struct = searcher.getIndexStructure();

		// Assemble response
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("indexName", indexName);
		response.put("status", searchMan.getIndexStatus(indexName));
		
		// Remove any empty settings
		response.removeEmptyMapValues();
		response.setCacheAllowed(false); // because status might change

		return response;
	}

}
