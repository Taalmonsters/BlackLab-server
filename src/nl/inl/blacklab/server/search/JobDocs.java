package nl.inl.blacklab.server.search;


/**
 * Represents a doc search operation.
 */
public class JobDocs extends JobWithDocs {

	public JobDocs(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws QueryException, IndexOpenException, InterruptedException {
		// First, execute blocking hits search.
		SearchParameters parNoSort = par.copyWithout("sort");
		JobWithHits hitsSearch = searchMan.searchHits(parNoSort, true);

		// Now, get per document results
		docResults = hitsSearch.getHits().perDocResults();
	}

}
