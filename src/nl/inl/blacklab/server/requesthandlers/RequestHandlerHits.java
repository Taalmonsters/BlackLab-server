package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Hit;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.HitsWindow;
import nl.inl.blacklab.search.IndexStructure;
import nl.inl.blacklab.search.Kwic;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.grouping.HitGroup;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.search.grouping.HitPropValue;
import nl.inl.blacklab.search.grouping.HitProperty;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectContextList;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.Job;
import nl.inl.blacklab.server.search.JobHitsGrouped;
import nl.inl.blacklab.server.search.JobHitsTotal;
import nl.inl.blacklab.server.search.JobHitsWindow;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchCache;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Request handler for hit results.
 */
public class RequestHandlerHits extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerHits.class);

	public RequestHandlerHits(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {

		debug(logger, "REQ hits: " + searchParam);

		// Do we want to view a single group after grouping?
		String groupBy = searchParam.get("group");
		if (groupBy == null)
			groupBy = "";
		String viewGroup = searchParam.get("viewgroup");
		if (viewGroup == null)
			viewGroup = "";
		Job search;
		JobHitsGrouped searchGrouped = null;
		JobHitsWindow searchWindow = null;
		HitsWindow window;
		HitGroup group = null;
		JobHitsTotal total = null;
		boolean block = getBoolParameter("block");
		if (groupBy.length() > 0 && viewGroup.length() > 0) {

			// TODO: clean up, do using JobHitsGroupedViewGroup or something (also cache sorted group!)

			// Yes. Group, then show hits from the specified group
			search = searchGrouped = searchMan.searchHitsGrouped(getUserId(), searchParam);
			if (block) {
				search.waitUntilFinished(SearchCache.MAX_SEARCH_TIME_SEC * 1000);
				if (!search.finished())
					return DataObject.errorObject("SEARCH_TIMED_OUT", "Search took too long, cancelled.");
			}

			// If search is not done yet, indicate this to the user
			if (!search.finished()) {
				return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getCheckAgainAdviceMinimumMs());
			}

			// Search is done; construct the results object
			HitGroups groups = searchGrouped.getGroups();

			HitPropValue viewGroupVal = null;
			viewGroupVal = HitPropValue.deserialize(searchGrouped.getHits(), viewGroup);
			if (viewGroupVal == null)
				return DataObject.errorObject("ERROR_IN_GROUP_VALUE", "Cannot deserialize group value: " + viewGroup);

			group = groups.getGroup(viewGroupVal);
			if (group == null)
				return DataObject.errorObject("GROUP_NOT_FOUND", "Group not found: " + viewGroup);

			String sortBy = searchParam.get("sort");
			HitProperty sortProp = sortBy != null && sortBy.length() > 0 ? HitProperty.deserialize(group.getHits(), sortBy) : null;
			Hits hitsSorted;
			if (sortProp != null)
				hitsSorted = group.getHits().sortedBy(sortProp);
			else
				hitsSorted = group.getHits();

			int first = searchParam.getInteger("first");
			int number = searchParam.getInteger("number");
			window = hitsSorted.window(first, number);

		} else {
			// Regular set of hits (no grouping first)

			search = searchWindow = searchMan.searchHitsWindow(getUserId(), searchParam);
			if (block) {
				search.waitUntilFinished(SearchCache.MAX_SEARCH_TIME_SEC * 1000);
				if (!search.finished())
					return DataObject.errorObject("SEARCH_TIMED_OUT", "Search took too long, cancelled.");
			}

			// Also determine the total number of hits (nonblocking)
			total = searchMan.searchHitsTotal(getUserId(), searchParam);

			// If search is not done yet, indicate this to the user
			if (!search.finished()) {
				return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getCheckAgainAdviceMinimumMs());
			}

			window = searchWindow.getWindow();
		}

		// Search is done; construct the results object
		Searcher searcher = search.getSearcher();
		IndexStructure struct = searcher.getIndexStructure();

		// The hits and document info
		DataObjectList hitList = new DataObjectList("hit");
		DataObjectMapAttribute docInfos = new DataObjectMapAttribute("doc-info", "pid");
		for (Hit hit: window) {
			DataObjectMapElement hitMap = new DataObjectMapElement();

			// Find pid
			Document document = searcher.document(hit.doc);
			String pid = searchMan.getDocumentPid(indexName, hit.doc, document);

			// Add basic hit info
			hitMap.put("doc-pid", pid);
			hitMap.put("start", hit.start);
			hitMap.put("end", hit.end);

			// Add KWIC info
			Kwic c = window.getKwic(hit);
			hitMap.put("left", new DataObjectContextList(c.getProperties(), c.getLeft()));
			hitMap.put("match", new DataObjectContextList(c.getProperties(), c.getMatch()));
			hitMap.put("right", new DataObjectContextList(c.getProperties(), c.getRight()));
			hitList.add(hitMap);

			// Add document info if we didn't already
			if (!docInfos.containsKey(hit.doc)) {
				docInfos.put(pid, getDocumentInfo(indexName, struct, searcher.document(hit.doc)));
			}
		}

		// The summary (done last because the count might be done by this time)
		DataObjectMapElement summary = new DataObjectMapElement();
		Hits hits = searchWindow != null ? hits = searchWindow.getWindow().getOriginalHits() : group.getHits();
		boolean done = hits.doneFetchingHits();
		summary.put("search-param", searchParam.toDataObject());
		summary.put("search-time", search.executionTimeMillis());
		if (total != null)
			summary.put("count-time", total.executionTimeMillis());
		summary.put("still-counting", !done);
		summary.put("number-of-hits", hits.countSoFarHitsCounted());
		summary.put("number-of-hits-retrieved", hits.countSoFarHitsRetrieved());
		summary.put("number-of-docs", hits.countSoFarDocsCounted());
		summary.put("number-of-docs-retrieved", hits.countSoFarDocsRetrieved());
		summary.put("window-first-result", window.first());
		summary.put("requested-window-size", searchParam.getInteger("number"));
		summary.put("actual-window-size", window.size());
		summary.put("window-has-previous", window.hasPrevious());
		summary.put("window-has-next", window.hasNext());

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("hits", hitList);
		response.put("doc-infos", docInfos);

		return response;
	}

}
