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
		JobDocsGrouped search = searchMan.searchDocsGrouped(user, searchParam);
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
		if (first < 0)
			first = 0;
		int number = searchParam.getInteger("number");
		if (number < 0 || number > searchMan.getMaxPageSize())
			number = searchMan.getDefaultPageSize();
		int i = 0;
		for (DocGroup group: groups) {
			if (i >= first && i < first + number) {
				DataObjectMapElement doGroup = new DataObjectMapElement();
				doGroup.put("identity", group.getIdentity().serialize());
				doGroup.put("identityDisplay", group.getIdentity().toString());
				doGroup.put("size", group.size());
				doGroups.add(doGroup);
			}
			i++;
		}

		// The summary
		DataObjectMapElement summary = new DataObjectMapElement();
		Hits hits = search.getDocResults().getOriginalHits();
		summary.put("searchParam", searchParam.toDataObject());
		summary.put("searchTime", search.executionTimeMillis());
		summary.put("stillCounting", false);
		summary.put("numberOfHits", hits.countSoFarHitsCounted());
		summary.put("numberOfHitsRetrieved", hits.countSoFarHitsRetrieved());
		summary.put("stoppedCountingHits", hits.maxHitsCounted());
		summary.put("stoppedRetrievingHits", hits.maxHitsRetrieved());
		summary.put("numberOfDocs", hits.countSoFarDocsCounted());
		summary.put("numberOfDocsRetrieved", hits.countSoFarDocsRetrieved());
		summary.put("numberOfGroups", groups.numberOfGroups());
		summary.put("windowFirstResult", first);
		summary.put("requestedWindowSize", number);
		summary.put("actualWindowSize", doGroups.size());
		summary.put("windowHasPrevious", first > 0);
		summary.put("windowHasNext", first + number < groups.numberOfGroups());
		summary.put("largestGroupSize", groups.getLargestGroupSize());

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("docGroups", doGroups);

		return response;
	}

}
