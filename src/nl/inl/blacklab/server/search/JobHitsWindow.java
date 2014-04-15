package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.HitsWindow;

/**
 * Represents searching for a window in a larger set of hits.
 */
public class JobHitsWindow extends Job {

	private HitsWindow window;

	public JobHitsWindow(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super(searchMan, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		JobWithHits hitsSearch = searchMan.searchHits(par, true);

		// Now, create a HitsWindow on these hits.
		Hits hits = hitsSearch.getHits();
		int first = par.getInteger("first");
		int number = par.getInteger("number");
		if (!hits.sizeAtLeast(first + 1)) {
			logger.debug("Parameter first (" + first + ") out of range; setting to 0");
			first = 0;
		}
		window = hits.window(first, number);
		int contextSize = par.getInteger("wordsaroundhit");
		if (contextSize > searchMan.maxContextSize) {
			logger.debug("Clamping context size to " + searchMan.maxContextSize + " (" + contextSize + " requested)");
			contextSize = searchMan.maxContextSize;
		}
		window.setContextSize(contextSize);

	}

	public HitsWindow getWindow() {
		return window;
	}

}
