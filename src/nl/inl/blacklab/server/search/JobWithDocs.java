package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.util.ThreadPriority.Level;

/**
 * A search job that produces a Hits object
 */
public class JobWithDocs extends Job {

	DocResults docResults;

	public JobWithDocs(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	public DocResults getDocResults() {
		return docResults;
	}

	@Override
	public DataObjectMapElement toDataObject(boolean debugInfo) {
		DataObjectMapElement d = super.toDataObject(debugInfo);
		d.put("countDocsRetrieved", docResults == null || docResults.getOriginalHits() == null ? -1 : docResults.getOriginalHits().countSoFarDocsRetrieved());
		return d;
	}

	@Override
	protected void setPriorityInternal() {
		setDocsPriority(docResults);
	}

	@Override
	public Level getPriorityOfResultsObject() {
		return docResults == null ? Level.RUNNING : docResults.getPriorityLevel();
	}

	@Override
	protected void cleanup() {
		docResults = null;
		super.cleanup();
	}

}
