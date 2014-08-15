package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * Represents finding the total number of hits.
 */
public class JobHitsTotal extends Job {

	private JobWithHits hitsSearch;

	public JobHitsTotal(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		hitsSearch = searchMan.searchHits(userId, par);
		waitForJobToFinish(hitsSearch);

		// Get the total number of hits (we ignore the value because you can monitor progress
		// and get the final total through the getHits() method yourself.
		Hits hits = hitsSearch.getHits();
		hits.size();
	}

	/**
	 * Returns the Hits object when available.
	 *
	 * @return the Hits object, or null if not available yet.
	 */
	public Hits getHits() {
		return hitsSearch != null ? hitsSearch.getHits() : null;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("hitsCounted", hitsSearch != null && hitsSearch.getHits() != null ? hitsSearch.getHits().countSoFarHitsCounted() : -1);
		return d;
	}

}
