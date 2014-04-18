package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * A search job that produces a Hits object
 */
public class JobWithHits extends Job {

	/** The hits found */
	protected Hits hits;

	public JobWithHits(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	public Hits getHits() {
		return hits;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("count-hits-retrieved", hits == null ? -1 : hits.countSoFarDocsRetrieved());
		return d;
	}

}
