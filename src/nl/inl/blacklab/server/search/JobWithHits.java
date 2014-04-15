package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;

/**
 * A search job that produces a Hits object
 */
public class JobWithHits extends Job {

	/** The hits found */
	protected Hits hits;

	public JobWithHits(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	public Hits getHits() {
		return hits;
	}

}
