package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;

/**
 * A search job that produces a Hits object
 */
public class JobWithDocs extends Job {

	DocResults docResults;

	public JobWithDocs(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	public DocResults getDocResults() {
		return docResults;
	}

}
