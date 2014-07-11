package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * Represents a docs search and sort operation.
 */
public class JobDocsSorted extends JobWithDocs {

	public JobDocsSorted(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking docs search.
		SearchParameters parNoSort = par.copyWithout("sort");
		JobWithDocs search = searchMan.searchDocs(userId, parNoSort);
		waitForJobToFinish(search);

		// Now, sort the docs.
		DocResults docsUnsorted = search.getDocResults();
		String sortBy = par.getString("sort");
		if (sortBy == null)
			sortBy = "";
		boolean reverse = false;
		if (sortBy.length() > 0 && sortBy.charAt(0) == '-') {
			reverse = true;
			sortBy = sortBy.substring(1);
		}
		DocProperty sortProp = DocProperty.deserialize(sortBy);
		if (sortProp == null)
			throw new QueryException("UNKNOWN_SORT_PROPERTY", "Unknown sort property '" + sortBy + "'.");
		docsUnsorted.sort(sortProp, reverse); // TODO: add .sortedBy() same as in Hits
		docResults = docsUnsorted; // client can use results
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("number-of-doc-results", docResults == null ? -1 : docResults.size());
		return d;
	}

}
