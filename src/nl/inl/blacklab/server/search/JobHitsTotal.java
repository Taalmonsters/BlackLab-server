package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;

/**
 * Represents finding the total number of hits.
 */
public class JobHitsTotal extends Job {

	private JobWithHits hitsSearch;

	public JobHitsTotal(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		hitsSearch = searchMan.searchHits(par, true);

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

}
