package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.grouping.HitProperty;

/**
 * Represents a hits search and sort operation.
 */
public class JobHitsSorted extends JobWithHits {

	public JobHitsSorted(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		SearchParameters parNoSort = par.copyWithout("sort");
		JobWithHits hitsSearch = searchMan.searchHits(parNoSort, true);

		// Now, sort the hits.
		Hits hitsUnsorted = hitsSearch.getHits();
		HitProperty sortProp = HitProperty.deserialize(hitsUnsorted, par.get("sort"));
		hits = hitsUnsorted.sortedBy(sortProp);
	}

}
