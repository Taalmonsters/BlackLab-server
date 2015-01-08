package nl.inl.blacklab.server.search;

import nl.inl.blacklab.exceptions.BlsException;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

/**
 * Represents finding the total number of docs.
 */
public class JobDocsTotal extends Job {

	private JobWithDocs docsSearch;

	public JobDocsTotal(SearchManager searchMan, User user, SearchParameters par) throws IndexOpenException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, BlsException, InterruptedException  {
		// First, execute blocking docs search.
		docsSearch = searchMan.searchDocs(user, par);
		waitForJobToFinish(docsSearch);

		// Get the total number of docs (we ignore the return value because you can monitor progress
		// and get the final total through the getDocResults() method yourself.
		DocResults docResults = docsSearch.getDocResults();
		docResults.size();
		if (Thread.interrupted()) {
			throw new InterruptedException("Interrupted while determining total number of docs");
		}
	}

	/**
	 * Returns the DocResults object when available.
	 *
	 * @return the DocResults object, or null if not available yet.
	 */
	public DocResults getDocResults() {
		return docsSearch != null ? docsSearch.getDocResults() : null;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("docsCounted", docsSearch != null && docsSearch.getDocResults().getOriginalHits() != null ? docsSearch.getDocResults().getOriginalHits().countSoFarDocsCounted() : -1);
		return d;
	}

}
