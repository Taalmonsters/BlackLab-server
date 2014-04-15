package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Hit;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.HitsWindow;
import nl.inl.blacklab.search.IndexStructure;
import nl.inl.blacklab.search.Kwic;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectContextList;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectMapInt;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.JobHitsTotal;
import nl.inl.blacklab.server.search.JobHitsWindow;
import nl.inl.blacklab.server.search.QueryException;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Request handler for the "hitset" request.
 */
public class RequestHandlerHitset extends RequestHandler {
	@SuppressWarnings("unused")
	private static final Logger logger = Logger.getLogger(RequestHandlerHitset.class);

	public RequestHandlerHitset(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {

		// Get the window we're interested in
		JobHitsWindow search = searchMan.searchHitsWindow(searchParam, getBoolParameter("block"));

		// Also determine the total number of hits (nonblocking)
		JobHitsTotal total = searchMan.searchHitsTotal(searchParam, false);

		// If search is not done yet, indicate this to the user
		if (!search.finished()) {
			return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getDefaultCheckAgainAdviceMs());
		}

		// Search is done; construct the results object
		Searcher searcher = search.getSearcher();
		IndexStructure struct = searcher.getIndexStructure();
		HitsWindow window = search.getWindow();

		// The hits and document info
		DataObjectList hitList = new DataObjectList("hit");
		DataObjectMapInt docInfos = new DataObjectMapInt("docinfo", "id");
		for (Hit hit: window) {
			DataObjectMapElement hitMap = new DataObjectMapElement();
			hitMap.put("doc", hit.doc);
			hitMap.put("start", hit.start);
			hitMap.put("end", hit.end);

			Kwic c = window.getKwic(hit);
			hitMap.put("left", new DataObjectContextList(c.properties, c.left));
			hitMap.put("match", new DataObjectContextList(c.properties, c.match));
			hitMap.put("right", new DataObjectContextList(c.properties, c.right));
			hitList.add(hitMap);

			if (!docInfos.containsKey(hit.doc)) {
				Document document = searcher.document(hit.doc);
				DataObjectMapElement docInfo = new DataObjectMapElement();
				for (String metadataFieldName: struct.getMetadataFields()) {
					String value = document.get(metadataFieldName);
					if (value != null)
						docInfo.put(metadataFieldName, value);
				}
				docInfo.put("mayView", "yes"); // TODO: decide based on config/auth
				docInfos.put(hit.doc, docInfo);
			}
		}

		// The summary (done last because the count might be done by this time)
		DataObjectMapElement summary = new DataObjectMapElement();
		Hits hits = total.getHits();
		boolean done = hits.doneFetchingHits();
		summary.put("search-time", search.executionTimeMillis());
		summary.put("count-time", total.executionTimeMillis());
		summary.put("still-counting", !done);
		summary.put("number-of-results", hits.countSoFarHitsCounted());
		summary.put("number-of-results-retrieved", hits.countSoFarHitsRetrieved());
		summary.put("number-of-docs", hits.countSoFarDocsCounted());
		summary.put("number-of-docs-retrieved", hits.countSoFarDocsRetrieved());
		summary.put("first-result", window.first());
		summary.put("number-of-results", window.size());
		summary.put("has-previous", window.hasPrevious());
		summary.put("has-next", window.hasNext());

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("hits", hitList);
		response.put("docinfos", docInfos);

		return response;
	}

}
