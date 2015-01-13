package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.grouping.GroupProperty;
import nl.inl.blacklab.search.grouping.HitGroups;
import nl.inl.blacklab.search.grouping.HitProperty;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BadRequest;
import nl.inl.blacklab.server.exceptions.BlsException;

/**
 * Represents a hits search and sort operation.
 */
public class JobHitsGrouped extends Job {

	private HitGroups groups;

	private Hits hits;

	public JobHitsGrouped(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException, InterruptedException  {
		// First, execute blocking hits search.
		SearchParameters parNoGroup = par.copyWithout("group", "sort");
		JobWithHits hitsSearch = searchMan.searchHits(user, parNoGroup);
		waitForJobToFinish(hitsSearch);

		// Now, group the hits.
		hits = hitsSearch.getHits();
		String groupBy = par.getString("group");
		HitProperty groupProp = null;
		if (groupBy == null)
			groupBy = "";
		groupProp = HitProperty.deserialize(hits, groupBy);
		if (groupProp == null)
			throw new BadRequest("UNKNOWN_GROUP_PROPERTY", "Unknown group property '" + groupBy + "'.");
		HitGroups theGroups = hits.groupedBy(groupProp);

		String sortBy = par.getString("sort");
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

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("hitsRetrieved", hits == null ? -1 : hits.countSoFarHitsRetrieved());
		d.put("numberOfGroups", groups == null ? -1 : groups.numberOfGroups());
		return d;
	}

}
