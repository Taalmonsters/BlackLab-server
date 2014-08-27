package nl.inl.blacklab.server.search;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

/**
 * Represents a doc search operation.
 */
public class JobDocs extends JobWithDocs {

	public JobDocs(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws QueryException, IndexOpenException, InterruptedException {
		// First, execute blocking hits search.
		String patt = par.getString("patt");
		if (patt != null && patt.length() > 0) {
			SearchParameters parNoSort = par.copyWithout("sort");
			JobWithHits hitsSearch = searchMan.searchHits(userId, parNoSort);
			waitForJobToFinish(hitsSearch);
			// Now, get per document results
			docResults = hitsSearch.getHits().perDocResults();
		} else {
			// Documents only
			Query filterQuery = SearchManager.parseFilter(searcher.getAnalyzer(), par.getString("filter"), par.getString("filterlang"));
			if (filterQuery == null)
				filterQuery = new MatchAllDocsQuery();
			docResults = searcher.queryDocuments(filterQuery);
		}
	}

}
