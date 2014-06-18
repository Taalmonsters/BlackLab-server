package nl.inl.blacklab.server.search;

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

	public JobDocsWindow(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		JobWithDocs docsSearch = searchMan.searchDocs(userId, par);
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
		//TODO context size
//		int contextSize = par.getInteger("wordsaroundhit");
//		if (contextSize > searchMan.maxContextSize) {
//			debug(logger, "Clamping context size to " + searchMan.maxContextSize + " (" + contextSize + " requested)");
//			contextSize = searchMan.maxContextSize;
//		}
//		window.setContextSize(contextSize);

	}

	public DocResultsWindow getWindow() {
		return window;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("requested-window-size", requestedWindowSize);
		d.put("actual-window-size", window == null ? -1 : window.size());
		return d;
	}

}
