package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.grouping.GroupProperty;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.search.grouping.HitProperty;

/**
 * Represents a hits search and sort operation.
 */
public class JobHitsGrouped extends Job {

	private HitGroups groups;

	private Hits hits;

	public JobHitsGrouped(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		SearchParameters parNoGroup = par.copyWithout("group", "sort");
		JobWithHits hitsSearch = searchMan.searchHits(parNoGroup, true);

		// Now, group the hits.
		hits = hitsSearch.getHits();
		String groupBy = par.get("group");
		HitProperty groupProp = HitProperty.deserialize(hits, groupBy);
		if (groupProp == null)
			throw new QueryException("UNKNOWN_GROUP_PROPERTY", "Unknown group property '" + groupBy + "'");
		HitGroups theGroups = hits.groupedBy(groupProp);

		String sortBy = par.get("sort");
		if (sortBy == null)
			sortBy = "";
		boolean reverse = false;
		if (sortBy.length() > 0 && sortBy.charAt(0) == '-') {
			reverse = true;
			sortBy = sortBy.substring(1);
		}
		GroupProperty sortProp = GroupProperty.deserialize(sortBy);
		theGroups.sortGroups(sortProp, reverse);

		groups = theGroups; // we're done, caller can use the groups now
	}

	public HitGroups getGroups() {
		return groups;
	}

	public Hits getHits() {
		return hits;
	}

}
