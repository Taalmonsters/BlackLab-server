package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocGroupProperty;
import nl.inl.blacklab.perdocument.DocGroups;
import nl.inl.blacklab.perdocument.DocProperty;
import nl.inl.blacklab.perdocument.DocResults;

/**
 * Represents a hits search and sort operation.
 */
public class JobDocsGrouped extends Job {

	private DocGroups groups;

	private DocResults docResults;

	public JobDocsGrouped(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		SearchParameters parNoGroup = par.copyWithout("group", "sort");
		JobWithDocs docsSearch = searchMan.searchDocs(parNoGroup, true);

		// Now, group the hits.
		docResults = docsSearch.getDocResults();
		String groupBy = par.get("group");
		DocProperty groupProp = null;
		if (groupBy == null)
			groupBy = "";
		groupProp = DocProperty.deserialize(groupBy);
		if (groupProp == null)
			throw new QueryException("UNKNOWN_GROUP_PROPERTY", "Unknown group property '" + groupBy + "'");
		DocGroups theGroups = docResults.groupedBy(groupProp);

		String sortBy = par.get("sort");
		if (sortBy == null)
			sortBy = "";
		boolean reverse = false;
		if (sortBy.length() > 0 && sortBy.charAt(0) == '-') {
			reverse = true;
			sortBy = sortBy.substring(1);
		}
		DocGroupProperty sortProp = DocGroupProperty.deserialize(sortBy);
		theGroups.sortGroups(sortProp, reverse);

		groups = theGroups; // we're done, caller can use the groups now
	}

	public DocGroups getGroups() {
		return groups;
	}

	public DocResults getDocResults() {
		return docResults;
	}

}
