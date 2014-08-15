package nl.inl.blacklab.server.requesthandlers;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
import nl.inl.blacklab.search.indexstructure.MetadataFieldDesc;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
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
		IndexStructure struct = searcher.getIndexStructure();

		MetadataFieldDesc fd = struct.getMetadataFieldDesc(fieldName);
		Map<String, Integer> values = fd.getValueDistribution();
		boolean valueListComplete = fd.isValueListComplete();

		// Assemble response
		DataObjectMapAttribute doFieldValues = new DataObjectMapAttribute("value", "text");
		for (Map.Entry<String, Integer> e: values.entrySet()) {
			doFieldValues.put(e.getKey(), e.getValue());
		}
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("indexName", indexName);
		response.put("fieldName", fieldName);
		response.put("displayName", fd.getDisplayName());
		response.put("description", fd.getDescription());
		response.put("type", fd.getType().toString());
		response.put("analyzer", fd.getAnalyzer());
		response.put("unknownCondition", fd.getUnknownCondition().toString());
		response.put("unknownValue", fd.getUnknownValue());
		response.put("fieldValues", doFieldValues);
		response.put("valueListComplete", valueListComplete);
		return response;
	}

}
