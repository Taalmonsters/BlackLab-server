package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocPropertyComplexFieldLength;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.search.Concordance;
import nl.inl.blacklab.search.Hit;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.HitsWindow;
import nl.inl.blacklab.search.Kwic;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TermFrequency;
import nl.inl.blacklab.search.TermFrequencyList;
import nl.inl.blacklab.search.grouping.HitGroup;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.search.grouping.HitPropValue;
import nl.inl.blacklab.search.grouping.HitProperty;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectContextList;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectPlain;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.search.Job;
import nl.inl.blacklab.server.search.JobHitsGrouped;
import nl.inl.blacklab.server.search.JobHitsTotal;
import nl.inl.blacklab.server.search.JobHitsWindow;
import nl.inl.blacklab.server.search.SearchCache;
import nl.inl.blacklab.server.search.User;

import org.apache.lucene.document.Document;

/**
 * Request handler for hit results.
 */
public class RequestHandlerHits extends RequestHandler {
	public RequestHandlerHits(BlackLabServer servlet, HttpServletRequest request, User user, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public Response handle() throws BlsException {
		//logger.debug("@PERF RHHits: START");
		
		// Do we want to view a single group after grouping?
		String groupBy = searchParam.getString("group");
		if (groupBy == null)
			groupBy = "";
		String viewGroup = searchParam.getString("viewgroup");
		if (viewGroup == null)
			viewGroup = "";
		Job search = null;
		JobHitsGrouped searchGrouped = null;
		JobHitsWindow searchWindow = null;
		JobHitsTotal total = null;
		try {
			HitsWindow window;
			HitGroup group = null;
			boolean block = getBoolParameter("block");
			if (groupBy.length() > 0 && viewGroup.length() > 0) {
	
				// TODO: clean up, do using JobHitsGroupedViewGroup or something (also cache sorted group!)
	
				// Yes. Group, then show hits from the specified group
				searchGrouped = searchMan.searchHitsGrouped(user, searchParam);
				search = searchGrouped;
				search.incrRef();
				if (block) {
					//logger.debug("@PERF RHHits: block");
					search.waitUntilFinished(SearchCache.maxSearchTimeSec * 1000);
					if (!search.finished()) {
						//logger.debug("@PERF RHHits: block, timed out");
						return Response.searchTimedOut();
					}
					//logger.debug("@PERF RHHits: block, finished");
				}
	
				// If search is not done yet, indicate this to the user
				if (!search.finished()) {
					return Response.busy(servlet);
				}
	
				// Search is done; construct the results object
				HitGroups groups = searchGrouped.getGroups();
	
				HitPropValue viewGroupVal = null;
				viewGroupVal = HitPropValue.deserialize(searchGrouped.getHits(), viewGroup);
				if (viewGroupVal == null)
					return Response.badRequest("ERROR_IN_GROUP_VALUE", "Cannot deserialize group value: " + viewGroup);
	
				group = groups.getGroup(viewGroupVal);
				if (group == null)
					return Response.badRequest("GROUP_NOT_FOUND", "Group not found: " + viewGroup);
	
				String sortBy = searchParam.getString("sort");
				HitProperty sortProp = sortBy != null && sortBy.length() > 0 ? HitProperty.deserialize(group.getHits(), sortBy) : null;
				Hits hitsSorted;
				if (sortProp != null)
					hitsSorted = group.getHits().sortedBy(sortProp);
				else
					hitsSorted = group.getHits();
	
				int first = searchParam.getInteger("first");
				if (first < 0)
					first = 0;
				int number = searchParam.getInteger("number");
				if (number < 0 || number > searchMan.getMaxPageSize())
					number = searchMan.getDefaultPageSize();
				if (!hitsSorted.sizeAtLeast(first))
					return Response.badRequest("HIT_NUMBER_OUT_OF_RANGE", "Non-existent hit number specified.");
				window = hitsSorted.window(first, number);
	
			} else {
				// Regular set of hits (no grouping first)
	
				searchWindow = searchMan.searchHitsWindow(user, searchParam);
				search = searchWindow;
				search.incrRef();
				if (block) {
					search.waitUntilFinished(SearchCache.maxSearchTimeSec * 1000);
					if (!search.finished()) {
						return Response.searchTimedOut();
					}
				}
	
				// Also determine the total number of hits
				// (usually nonblocking, unless "waitfortotal=yes" was passed)
				total = searchMan.searchHitsTotal(user, searchParam);
				if (searchParam.getBoolean("waitfortotal")) {
					//logger.debug("@PERF RHHits: waitfortotal");
					total.waitUntilFinished(SearchCache.maxSearchTimeSec * 1000);
					if (!total.finished()) {
						//logger.debug("@PERF RHHits: waitfortotal timed out");
						return Response.searchTimedOut();
					}
					//logger.debug("@PERF RHHits: waitfortotal finished");
				}
	
				// If search is not done yet, indicate this to the user
				if (!search.finished()) {
					//logger.debug("@PERF RHHits: busy");
					return Response.busy(servlet);
				}
	
				//logger.debug("@PERF RHHits: get Window");
				window = searchWindow.getWindow();
				//logger.debug("@PERF RHHits: got Window");
			}
			
			if (searchParam.getString("calc").equals("colloc")) {
				//logger.debug("@PERF RHHits: colloc");
				return new Response(getCollocations(window.getOriginalHits()));
			}
			
			String parFacets = searchParam.getString("facets");
			DataObjectMapAttribute doFacets = null;
			DocResults perDocResults = null;
			if (parFacets != null && parFacets.length() > 0) {
				//logger.debug("@PERF RHHits: facets");
				// Now, group the docs according to the requested facets.
				perDocResults = window.getOriginalHits().perDocResults();
				doFacets = getFacets(perDocResults, parFacets);
			}
	
			Searcher searcher = search.getSearcher();
	
			boolean includeTokenCount = searchParam.getBoolean("includetokencount");
			int totalTokens = -1;
			IndexStructure struct = searcher.getIndexStructure();
			if (includeTokenCount) {
				//logger.debug("@PERF RHHits: token count");
				if (perDocResults == null)
					perDocResults = window.getOriginalHits().perDocResults();
				// Determine total number of tokens in result set
				String fieldName = struct.getMainContentsField().getName();
				DocProperty propTokens = new DocPropertyComplexFieldLength(fieldName);
				totalTokens = perDocResults.intSum(propTokens);
			}
	
			// Search is done; construct the results object
	
			// The hits and document info
			DataObjectList hitList = new DataObjectList("hit");
			DataObjectMapAttribute docInfos = new DataObjectMapAttribute("docInfo", "pid");
			//logger.debug("@PERF RHHits: construct results");
			for (Hit hit: window) {
				DataObjectMapElement hitMap = new DataObjectMapElement();
	
				// Find pid
				Document document = searcher.document(hit.doc);
				String pid = getDocumentPid(searcher, hit.doc, document);
	
				boolean useOrigContent = searchParam.getString("usecontent").equals("orig");
				
				// TODO: use RequestHandlerDocSnippet.getHitOrFragmentInfo()
				
				// Add basic hit info
				hitMap.put("docPid", pid);
				hitMap.put("start", hit.start);
				hitMap.put("end", hit.end);
	
				if (useOrigContent) {
					// Add concordance from original XML
					Concordance c = window.getConcordance(hit);
					hitMap.put("left", new DataObjectPlain(c.left()));
					hitMap.put("match", new DataObjectPlain(c.match()));
					hitMap.put("right", new DataObjectPlain(c.right()));
				} else {
					// Add KWIC info
					Kwic c = window.getKwic(hit);
					hitMap.put("left", new DataObjectContextList(c.getProperties(), c.getLeft()));
					hitMap.put("match", new DataObjectContextList(c.getProperties(), c.getMatch()));
					hitMap.put("right", new DataObjectContextList(c.getProperties(), c.getRight()));
				}
				hitList.add(hitMap);
	
				// Add document info if we didn't already
				if (!docInfos.containsKey(hit.doc)) {
					docInfos.put(pid, getDocumentInfo(searcher, searcher.document(hit.doc)));
				}
			}
			//logger.debug("@PERF RHHits: construct results DONE");
	
			// The summary (done last because the count might be done by this time)
			DataObjectMapElement summary = new DataObjectMapElement();
			Hits hits = searchWindow != null ? hits = searchWindow.getWindow().getOriginalHits() : group.getHits();
			boolean done = hits.doneFetchingHits();
			summary.put("searchParam", searchParam.toDataObject());
			summary.put("searchTime", (int)(search.userWaitTime() * 1000));
			if (total != null)
				summary.put("countTime", (int)(total.userWaitTime() * 1000));
			summary.put("stillCounting", !done);
			int totalHitsCounted = hits.countSoFarHitsCounted();
			if (total != null && total.threwException()) {
				// indicate that something went wrong while counting;
				// i.e. timeout
				totalHitsCounted = -1;
			}
			summary.put("numberOfHits", totalHitsCounted);
			summary.put("numberOfHitsRetrieved", hits.countSoFarHitsRetrieved());
			summary.put("stoppedCountingHits", hits.maxHitsCounted());
			summary.put("stoppedRetrievingHits", hits.maxHitsRetrieved());
			summary.put("numberOfDocs", hits.countSoFarDocsCounted());
			summary.put("numberOfDocsRetrieved", hits.countSoFarDocsRetrieved());
			summary.put("windowFirstResult", window.first());
			summary.put("requestedWindowSize", searchParam.getInteger("number"));
			summary.put("actualWindowSize", window.size());
			summary.put("windowHasPrevious", window.hasPrevious());
			summary.put("windowHasNext", window.hasNext());
			if (includeTokenCount)
				summary.put("tokensInMatchingDocuments", totalTokens);
			summary.put("docFields", RequestHandler.getDocFields(struct));
	
			// Assemble all the parts
			DataObjectMapElement response = new DataObjectMapElement();
			response.put("summary", summary);
			response.put("hits", hitList);
			response.put("docInfos", docInfos);
			if (doFacets != null)
				response.put("facets", doFacets);
	
			return new Response(response);
		} finally {
			if (search != null)
				search.decrRef();
			if (searchWindow != null)
				searchWindow.decrRef();
			if (searchGrouped != null)
				searchGrouped.decrRef();
			if (total != null)
				total.decrRef();
		}
	}

	private DataObject getCollocations(Hits originalHits) {
		originalHits.setContextSize(searchParam.getInteger("wordsaroundhit"));
		DataObjectMapAttribute doTokenFreq = new DataObjectMapAttribute("token", "text");
		TermFrequencyList tfl = originalHits.getCollocations();
		tfl.sort();
		for (TermFrequency tf: tfl) {
			doTokenFreq.put(tf.term, tf.frequency);
		}
		
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("tokenFrequencies", doTokenFreq);
		return response;
	}

}
