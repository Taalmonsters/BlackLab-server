package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * A search job that produces a Hits object
 */
public class JobWithDocs extends Job {

	DocResults docResults;

	public JobWithDocs(SearchManager searchMan, User user, SearchParameters par) throws IndexOpenException {
		super(searchMan, user, par);
	}

	public DocResults getDocResults() {
		return docResults;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("countDocsRetrieved", docResults == null || docResults.getOriginalHits() == null ? -1 : docResults.getOriginalHits().countSoFarDocsRetrieved());
		return d;
	}

}
