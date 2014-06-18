package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.perdocument.DocGroup;
import nl.inl.blacklab.perdocument.DocGroups;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.JobDocsGrouped;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchCache;

import org.apache.log4j.Logger;

/**
 * Request handler for grouped doc results.
 */
public class RequestHandlerDocsGrouped extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerDocsGrouped.class);

	public RequestHandlerDocsGrouped(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {
		debug(logger, "REQ docsgrouped: " + searchParam);

		// Get the window we're interested in
		JobDocsGrouped search = searchMan.searchDocsGrouped(getUserId(), searchParam);
		if (getBoolParameter("block")) {
			search.waitUntilFinished(SearchCache.MAX_SEARCH_TIME_SEC * 1000);
			if (!search.finished())
				return DataObject.errorObject("SEARCH_TIMED_OUT", "Search took too long, cancelled.");
		}

		// If search is not done yet, indicate this to the user
		if (!search.finished()) {
			return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getCheckAgainAdviceMinimumMs());
		}

		// Search is done; construct the results object
		DocGroups groups = search.getGroups();

		DataObjectList doGroups = null;
		// The list of groups found
		// TODO paging..?
		doGroups = new DataObjectList("docgroup");
		int first = searchParam.getInteger("first");
		int number = searchParam.getInteger("number");
		int i = 0;
		for (DocGroup group: groups) {
			if (i >= first && i < first + number) {
				DataObjectMapElement doGroup = new DataObjectMapElement();
				doGroup.put("identity", group.getIdentity().serialize());
				doGroup.put("identity-display", group.getIdentity().toString());
				doGroup.put("size", group.size());
				doGroups.add(doGroup);
			}
			i++;
		}

		// The summary
		DataObjectMapElement summary = new DataObjectMapElement();
		Hits hits = search.getDocResults().getOriginalHits();
		summary.put("search-time", search.executionTimeMillis());
		summary.put("still-counting", false);
		summary.put("number-of-hits", hits.countSoFarHitsCounted());
		summary.put("number-of-hits-retrieved", hits.countSoFarHitsRetrieved());
		summary.put("number-of-docs", hits.countSoFarDocsCounted());
		summary.put("number-of-docs-retrieved", hits.countSoFarDocsRetrieved());
		summary.put("number-of-groups", groups.numberOfGroups());
		summary.put("window-first-result", first);
		summary.put("requested-window-size", number);
		summary.put("actual-window-size", doGroups.size());
		summary.put("window-has-previous", first > 0);
		summary.put("window-has-next", first + number < groups.numberOfGroups());
		summary.put("largest-group-size", groups.getLargestGroupSize());

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("docgroups", doGroups);
		/*response.put("hits", hitList);
		response.put("docinfos", docInfos);*/

		return response;
	}

}
