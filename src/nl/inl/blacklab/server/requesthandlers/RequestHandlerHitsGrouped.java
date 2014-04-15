package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.grouping.HitGroup;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.JobHitsGrouped;
import nl.inl.blacklab.server.search.QueryException;

/**
 * Request handler for the "hitset" request.
 */
public class RequestHandlerHitsGrouped extends RequestHandler {
	//private static final Logger logger = Logger.getLogger(RequestHandlerHitset.class);

	public RequestHandlerHitsGrouped(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {
		logger.debug("REQ hitsgrouped: " + searchParam);

		// Get the window we're interested in
		JobHitsGrouped search = searchMan.searchHitsGrouped(searchParam, getBoolParameter("block"));

		// If search is not done yet, indicate this to the user
		if (!search.finished()) {
			return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getDefaultCheckAgainAdviceMs());
		}

		// Search is done; construct the results object
		HitGroups groups = search.getGroups();

		DataObjectList doGroups = null;
		// The list of groups found
		// TODO paging..?
		doGroups = new DataObjectList("group");
		for (HitGroup group: groups) {
			DataObjectMapElement doGroup = new DataObjectMapElement();
			doGroup.put("identity", group.getIdentity().serialize());
			doGroup.put("identity-human-readable", group.getIdentity().toString());
			doGroup.put("size", group.size());
			doGroups.add(doGroup);
		}

		// The summary
		DataObjectMapElement summary = new DataObjectMapElement();
		Hits hits = search.getHits();
		summary.put("search-time", search.executionTimeMillis());
		summary.put("still-counting", false);
		summary.put("number-of-results", hits.countSoFarHitsCounted());
		summary.put("number-of-results-retrieved", hits.countSoFarHitsRetrieved());
		summary.put("number-of-docs", hits.countSoFarDocsCounted());
		summary.put("number-of-docs-retrieved", hits.countSoFarDocsRetrieved());
		/*
		summary.put("window-first-result", window.first());
		summary.put("window-size", window.size());
		summary.put("window-has-previous", window.hasPrevious());
		summary.put("window-has-next", window.hasNext());
		*/

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("groups", doGroups);
		/*response.put("hits", hitList);
		response.put("docinfos", docInfos);*/

		return response;
	}

}
