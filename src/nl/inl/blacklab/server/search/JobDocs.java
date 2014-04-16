package nl.inl.blacklab.server.search;

import org.apache.lucene.search.Query;

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
		String patt = par.get("patt");
		if (patt != null && patt.length() > 0) {
			SearchParameters parNoSort = par.copyWithout("sort");
			JobWithHits hitsSearch = searchMan.searchHits(parNoSort);
			waitForJobToFinish(hitsSearch);
			// Now, get per document results
			docResults = hitsSearch.getHits().perDocResults();
		} else {
			Query filterQuery = SearchManager.parseFilter(par.get("filter"), par.get("filterlang"));
			docResults = searcher.queryDocuments(filterQuery);
		}
	}

}
