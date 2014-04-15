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
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectMapInt;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.Job;
import nl.inl.blacklab.server.search.JobHitsGrouped;
import nl.inl.blacklab.server.search.JobHitsTotal;
import nl.inl.blacklab.server.search.JobHitsWindow;
import nl.inl.blacklab.server.search.QueryException;

import org.apache.lucene.document.Document;

/**
 * Request handler for the "hitset" request.
 */
public class RequestHandlerHits extends RequestHandler {
	//private static final Logger logger = Logger.getLogger(RequestHandlerHitset.class);

	public RequestHandlerHits(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {
		logger.debug("REQ hits: " + searchParam);

		// Do we want to view a single group after grouping?
		String groupBy = getStringParameter("group");
		if (groupBy == null)
			groupBy = "";
		String viewGroup = getStringParameter("viewgroup");
		if (viewGroup == null)
			viewGroup = "";
		Job search;
		JobHitsGrouped searchGrouped = null;
		JobHitsWindow searchWindow = null;
		HitsWindow window;
		HitGroup group = null;
		JobHitsTotal total = null;
		if (groupBy.length() > 0 && viewGroup.length() > 0) {

			// TODO: clean up, do using JobHitsGroupedViewGroup or something (also cache sorted group!)

			// Yes. Group, then show hits from the specified group
			search = searchGrouped = searchMan.searchHitsGrouped(searchParam, getBoolParameter("block"));

			// If search is not done yet, indicate this to the user
			if (!search.finished()) {
				return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getDefaultCheckAgainAdviceMs());
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

			String sortBy = getStringParameter("sort");
			HitProperty sortProp = sortBy != null && sortBy.length() > 0 ? HitProperty.deserialize(group.getHits(), sortBy) : null;
			Hits hitsSorted;
			if (sortProp != null)
				hitsSorted = group.getHits().sortedBy(sortProp);
			else
				hitsSorted = group.getHits();

			window = hitsSorted.window(getIntParameter("first"), getIntParameter("number"));

		} else {
			// Regular set of hits (no grouping first)

			search = searchWindow = searchMan.searchHitsWindow(searchParam, getBoolParameter("block"));

			// Also determine the total number of hits (nonblocking)
			total = searchMan.searchHitsTotal(searchParam, false);

			// If search is not done yet, indicate this to the user
			if (!search.finished()) {
				return DataObject.statusObject("WORKING", "Searching, please wait...", servlet.getSearchManager().getDefaultCheckAgainAdviceMs());
			}

			window = searchWindow.getWindow();
		}

		// Search is done; construct the results object
		Searcher searcher = search.getSearcher();
		IndexStructure struct = searcher.getIndexStructure();

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
		Hits hits = searchWindow != null ? total.getHits() : group.getHits();
		boolean done = hits.doneFetchingHits();
		summary.put("search-time", search.executionTimeMillis());
		if (total != null)
			summary.put("count-time", total.executionTimeMillis());
		summary.put("still-counting", !done);
		summary.put("number-of-results", hits.countSoFarHitsCounted());
		summary.put("number-of-results-retrieved", hits.countSoFarHitsRetrieved());
		summary.put("number-of-docs", hits.countSoFarDocsCounted());
		summary.put("number-of-docs-retrieved", hits.countSoFarDocsRetrieved());
		summary.put("window-first-result", window.first());
		summary.put("window-size", window.size());
		summary.put("window-has-previous", window.hasPrevious());
		summary.put("window-has-next", window.hasNext());

		// Assemble all the parts
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("summary", summary);
		response.put("hits", hitList);
		response.put("docinfos", docInfos);

		return response;
	}

}
