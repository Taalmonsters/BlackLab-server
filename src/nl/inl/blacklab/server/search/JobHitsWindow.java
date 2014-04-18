package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.HitsWindow;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

import org.apache.log4j.Logger;

/**
 * Represents searching for a window in a larger set of hits.
 */
public class JobHitsWindow extends Job {
	@SuppressWarnings("hiding")
	protected static final Logger logger = Logger.getLogger(JobHitsWindow.class);

	private HitsWindow window;

	public JobHitsWindow(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws IndexOpenException, QueryException, InterruptedException  {
		// First, execute blocking hits search.
		JobWithHits hitsSearch = searchMan.searchHits(userId, par);
		waitForJobToFinish(hitsSearch);

		// Now, create a HitsWindow on these hits.
		Hits hits = hitsSearch.getHits();
		int first = par.getInteger("first");
		int number = par.getInteger("number");
		if (!hits.sizeAtLeast(first + 1)) {
			debug(logger, "Parameter first (" + first + ") out of range; setting to 0");
			first = 0;
		}
		window = hits.window(first, number);
		int contextSize = par.getInteger("wordsaroundhit");
		int maxContextSize = searchMan.getMaxContextSize();
		if (contextSize > maxContextSize) {
			debug(logger, "Clamping context size to " + maxContextSize + " (" + contextSize + " requested)");
			contextSize = maxContextSize;
		}
		window.setContextSize(contextSize);

	}

	public HitsWindow getWindow() {
		return window;
	}

	@Override
	public DataObjectMapElement toDataObject() {
		DataObjectMapElement d = super.toDataObject();
		d.put("window-size", window == null ? -1 : window.size());
		return d;
	}

}
