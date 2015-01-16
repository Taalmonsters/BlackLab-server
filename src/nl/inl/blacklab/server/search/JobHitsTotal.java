package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;

/**
 * Represents finding the total number of hits.
 */
public class JobHitsTotal extends Job {

	private Hits hits = null;

	public JobHitsTotal(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException, InterruptedException  {
		// First, execute blocking hits search.
		JobWithHits hitsSearch = searchMan.searchHits(user, par);
		try {
			waitForJobToFinish(hitsSearch);
	
			// Get the total number of hits (we ignore the value because you can monitor progress
			// and get the final total through the getHits() method yourself.
			hits = hitsSearch.getHits();
		} finally {
			hitsSearch.decrRef();
			hitsSearch = null;
		}
		hits.size();
		if (Thread.interrupted()) {
			throw new InterruptedException("Interrupted while determining total number of hits");
		}
	}

	/**
	 * Returns the Hits object when available.
	 *
	 * @return the Hits object, or null if not available yet.
	 */
	public Hits getHits() {
		return hits;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("hitsCounted", hits != null ? hits.countSoFarHitsCounted() : -1);
		return d;
	}

	@Override
	protected void cleanup() {
		hits = null;
		super.cleanup();
	}

}
