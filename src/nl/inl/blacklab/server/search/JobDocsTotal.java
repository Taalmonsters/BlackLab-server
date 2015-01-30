package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.exceptions.ServiceUnavailable;

/**
 * Represents finding the total number of docs.
 */
public class JobDocsTotal extends Job {

	//private JobWithDocs docsSearch;
	
	private DocResults docResults = null;

	public JobDocsTotal(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException {
		// First, execute blocking docs search.
		JobWithDocs docsSearch = searchMan.searchDocs(user, par);
		try {
			waitForJobToFinish(docsSearch);
	
			// Get the total number of docs (we ignore the return value because you can monitor progress
			// and get the final total through the getDocResults() method yourself.
			docResults = docsSearch.getDocResults();
		} finally {
			docsSearch.decrRef();
			docsSearch = null;
		}
		docResults.size();
		if (Thread.interrupted()) {
			throw new ServiceUnavailable("Determining total number of docs took too long, cancelled");
		}
	}

	@Override
	protected void setPriorityInternal() {
		if (docResults != null)
			setDocsPriority(docResults);
	}

	/**
	 * Returns the DocResults object when available.
	 *
	 * @return the DocResults object, or null if not available yet.
	 */
	public DocResults getDocResults() {
		return docResults;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("docsCounted", docResults.getOriginalHits() != null ? docResults.getOriginalHits().countSoFarDocsCounted() : -1);
		return d;
	}

	@Override
	protected void cleanup() {
		docResults = null;
		super.cleanup();
	}

}
