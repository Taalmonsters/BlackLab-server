package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocResults;

/**
 * Represents a docs search and sort operation.
 */
public class JobDocsSorted extends JobWithDocs {

	public JobDocsSorted(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking docs search.
		SearchParameters parNoSort = par.copyWithout("sort");
		JobWithDocs search = searchMan.searchDocs(parNoSort, true);

		// Now, sort the docs.
		DocResults docsUnsorted = search.getDocResults();
		String sortBy = par.get("sort");
		if (sortBy == null)
			sortBy = "";
		boolean reverse = false;
		if (sortBy.length() > 0 && sortBy.charAt(0) == '-') {
			reverse = true;
			sortBy = sortBy.substring(1);
		}
		DocProperty sortProp = DocProperty.deserialize(sortBy);
		if (sortProp == null)
			throw new QueryException("UNKNOWN_SORT_PROPERTY", "Unknown sort property '" + sortBy + "'");
		docsUnsorted.sort(sortProp, reverse); // TODO: add .sortedBy() same as in Hits
		docResults = docsUnsorted; // client can use results
	}

}
