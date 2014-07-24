package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.IndexStructure;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerFieldInfo extends RequestHandler {

	public RequestHandlerFieldInfo(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {

		int i = urlPathInfo.indexOf('/');
		String fieldName = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (fieldName.length() == 0) {
			// FIXME show list of fields?
			throw new QueryException("NO_DOC_ID", "Specify document pid.");
		}

		Searcher searcher = searchMan.getSearcher(indexName);
		@SuppressWarnings("unused")
		IndexStructure struct = searcher.getIndexStructure();

		// Assemble response
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("index-name", indexName);
		response.put("field-name", fieldName);
		response.put("field-values", new DataObjectList("value", new DataObject[] {}));

		return response;
	}

}
