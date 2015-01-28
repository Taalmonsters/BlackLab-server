package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.perdocument.DocResultsWindow;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;

import org.apache.log4j.Logger;

/**
 * Represents searching for a window in a larger set of hits.
 */
public class JobDocsWindow extends Job {
	@SuppressWarnings("hiding")
	protected static final Logger logger = Logger.getLogger(JobDocsWindow.class);

	private DocResults sourceResults;

	private DocResultsWindow window;

	private int requestedWindowSize;

	public JobDocsWindow(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException, InterruptedException  {
		// First, execute blocking docs search.
		JobWithDocs docsSearch = searchMan.searchDocs(user, par);
		try {
			waitForJobToFinish(docsSearch);
	
			// Now, create a HitsWindow on these hits.
			sourceResults = docsSearch.getDocResults();
		} finally {
			docsSearch.decrRef();
			docsSearch = null;
		}
		int first = par.getInteger("first");
		requestedWindowSize = par.getInteger("number");
		if (!sourceResults.sizeAtLeast(first + 1)) {
			debug(logger, "Parameter first (" + first + ") out of range; setting to 0");
			first = 0;
		}
		window = sourceResults.window(first, requestedWindowSize);
	}

	@Override
	protected void setPriorityInternal() {
		if (sourceResults != null)
			setDocsPriority(sourceResults);
	}

	public DocResultsWindow getWindow() {
		return window;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("requestedWindowSize", requestedWindowSize);
		d.put("actualWindowSize", window == null ? -1 : window.size());
		return d;
	}

	@Override
	protected void cleanup() {
		window = null;
		super.cleanup();
	}

}
