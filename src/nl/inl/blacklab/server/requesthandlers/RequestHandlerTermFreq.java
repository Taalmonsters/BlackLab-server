package nl.inl.blacklab.server.requesthandlers;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TermFrequency;
import nl.inl.blacklab.search.TermFrequencyList;
import nl.inl.blacklab.search.indexstructure.ComplexFieldDesc;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchManager;

import org.apache.lucene.search.Query;

/**
 * Request handler for term frequencies for a set of documents.
 */
public class RequestHandlerTermFreq extends RequestHandler {
	
	public RequestHandlerTermFreq(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {
		//TODO: use background job?

		Searcher searcher = getSearcher();
		IndexStructure struct = searcher.getIndexStructure();
		ComplexFieldDesc cfd = struct.getMainContentsField();
		String propName = searchParam.getString("property");
		boolean sensitive = searchParam.getBoolean("sensitive");
		Query q = SearchManager.parseFilter(searcher, searchParam.getString("filter"), searchParam.getString("filterlang"));
		Map<String, Integer> freq = searcher.termFrequencies(q, cfd.getName(), propName, sensitive ? "s" : "i");
		
		TermFrequencyList tfl = new TermFrequencyList(freq.size());
		for (Map.Entry<String, Integer> e: freq.entrySet()) {
			tfl.add(new TermFrequency(e.getKey(), e.getValue()));
		}
		tfl.sort();
		
		int first = searchParam.getInteger("first");
		if (first < 0 || first >= tfl.size())
			first = 0;
		int number = searchParam.getInteger("number");
		if (number < 0 || number > searchMan.getMaxPageSize())
			number = searchMan.getDefaultPageSize();
		int last = first + number;
		if (last > tfl.size())
			last = tfl.size();
		DataObjectMapAttribute termFreq = new DataObjectMapAttribute("term", "text");
		for (TermFrequency tf: tfl.subList(first, last)) {
			termFreq.put(tf.term, tf.frequency);
		}

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("termFreq", termFreq);

		return response;
	}


}
