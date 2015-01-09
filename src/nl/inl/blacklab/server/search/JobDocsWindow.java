package nl.inl.blacklab.server.search;

import nl.inl.blacklab.exceptions.BlsException;
import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.perdocument.DocResultsWindow;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

import org.apache.log4j.Logger;

/**
 * Represents searching for a window in a larger set of hits.
 */
public class JobDocsWindow extends Job {
	@SuppressWarnings("hiding")
	protected static final Logger logger = Logger.getLogger(JobDocsWindow.class);

	private DocResultsWindow window;

	private int requestedWindowSize;

	public JobDocsWindow(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super(searchMan, user, par);
	}

	@Override
	public void performSearch() throws BlsException, InterruptedException  {
		// First, execute blocking docs search.
		JobWithDocs docsSearch = searchMan.searchDocs(user, par);
		waitForJobToFinish(docsSearch);

		// Now, create a HitsWindow on these hits.
		DocResults docResults = docsSearch.getDocResults();
		int first = par.getInteger("first");
		requestedWindowSize = par.getInteger("number");
		if (!docResults.sizeAtLeast(first + 1)) {
			debug(logger, "Parameter first (" + first + ") out of range; setting to 0");
			first = 0;
		}
		window = docResults.window(first, requestedWindowSize);
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

}
