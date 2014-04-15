package nl.inl.blacklab.server.search;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.perdocument.DocResultsWindow;

/**
 * Represents searching for a window in a larger set of hits.
 */
public class JobDocsWindow extends Job {

	private DocResultsWindow window;

	public JobDocsWindow(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		JobWithDocs docsSearch = searchMan.searchDocs(par, true);

		// Now, create a HitsWindow on these hits.
		DocResults docResults = docsSearch.getDocResults();
		int first = par.getInteger("first");
		int number = par.getInteger("number");
		if (!docResults.sizeAtLeast(first + 1)) {
			logger.debug("Parameter first (" + first + ") out of range; setting to 0");
			first = 0;
		}
		window = docResults.window(first, number);
//		int contextSize = par.getInteger("wordsaroundhit");
//		if (contextSize > searchMan.maxContextSize) {
//			logger.debug("Clamping context size to " + searchMan.maxContextSize + " (" + contextSize + " requested)");
//			contextSize = searchMan.maxContextSize;
//		}
//		window.setContextSize(contextSize);

	}

	public DocResultsWindow getWindow() {
		return window;
	}

}
