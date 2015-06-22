package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.util.ThreadPriority.Level;

/**
 * A search job that produces a Hits object
 */
public class JobWithHits extends Job {

	/** The hits found */
	protected Hits hits;

	public JobWithHits(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	public Hits getHits() {
		return hits;
	}

	@Override
	public DataObjectMapElement toDataObject(boolean debugInfo) {
		DataObjectMapElement d = super.toDataObject(debugInfo);
		d.put("countHitsRetrieved", hits == null ? -1 : hits.countSoFarDocsRetrieved());
		return d;
	}

	@Override
	protected void setPriorityInternal() {
		setHitsPriority(hits);
	}

	@Override
	public Level getPriorityOfResultsObject() {
		return hits == null ? Level.RUNNING : hits.getPriorityLevel();
	}

	@Override
	protected void cleanup() {
		hits = null;
		super.cleanup();
	}

}
