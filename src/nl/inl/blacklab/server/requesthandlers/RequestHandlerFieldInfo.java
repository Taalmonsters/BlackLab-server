package nl.inl.blacklab.server.requesthandlers;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
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
		
		final int MAX_TERMS = 50;
		List<String> fieldTerms = searcher.getFieldTerms(fieldName, MAX_TERMS + 1);
		boolean moreThanMaxTerms = fieldTerms.size() > MAX_TERMS;
		if (moreThanMaxTerms)
			fieldTerms = fieldTerms.subList(0, MAX_TERMS); 
		DataObjectList doTerms = new DataObjectList("term");
		for (String term: fieldTerms)
			doTerms.add(term);

		// Assemble response
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("index-name", indexName);
		response.put("field-name", fieldName);
		response.put("field-terms", doTerms);
		response.put("terms-list-complete", !moreThanMaxTerms);

		return response;
	}

}
