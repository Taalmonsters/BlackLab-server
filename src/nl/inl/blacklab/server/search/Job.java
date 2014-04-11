package nl.inl.blacklab.server.search;

import nl.inl.blacklab.search.Searcher;
import nl.inl.util.ExUtil;

import org.apache.log4j.Logger;

public abstract class Job implements Comparable<Job> {

	/** Log4j logger object */
	protected static final Logger logger = Logger.getLogger(JobHits.class);

	/**
	 * Create a new Search (subclass) object to carry out the specified search,
	 * and call the perform() method to start the search.
	 *
	 * @param searchMan the servlet
	 * @param par search parameters
	 * @return the new Search object
	 * @throws IndexOpenException
	 * @throws QueryException
	 */
	public static Job create(SearchManager searchMan, SearchParameters par) throws IndexOpenException, QueryException {
		Job search = null;
		String jobClass = par.get("jobclass");
		if (jobClass.equals("JobHits")) {
			search = new JobHits(searchMan, par);
		} else if (jobClass.equals("JobHitsWindow")) {
			search = new JobHitsWindow(searchMan, par);
		} else if (jobClass.equals("JobHitsTotal")) {
			search = new JobHitsTotal(searchMan, par);
		} else
			throw new QueryException("INTERNAL_ERROR", "Unknown job class '" + jobClass + "'");

		// TODO: implement other search types
		//   use Map<String, Class<? extends Search>> ?

		return search;
	}

	/** When this job was started (or -1 if not started yet) */
	protected long startedAt;

	/** When this job was finished (or -1 if not finished yet) */
	protected long finishedAt;

	/** The last time the results of this search were accessed (for caching) */
	protected long lastAccessed;

	/** The index searcher */
	protected Searcher searcher;

	/** Has perform() been called or not? Don't call it twice! */
	private boolean performCalled = false;

	/** Thread object carrying out the search, if performing the search. */
	private SearchThread searchThread = null;

	/** Parameters uniquely identifying this search */
	protected SearchParameters par;

	/** The servlet */
	protected SearchManager searchMan;

	public Job(SearchManager searchMan, SearchParameters par) throws IndexOpenException {
		super();
		this.par = par;
		this.searchMan = searchMan;
		searcher = searchMan.getSearcher(par.get("indexname"));
		resetLastAccessed();
		startedAt = -1;
		finishedAt = -1;
	}

	public Searcher getSearcher() {
		return searcher;
	}

	/**
	 * Compare based on last access time.
	 *
	 * @param o the other search, to compare to
	 * @return -1 if this search is staler than o;
	 *   1 if this search is fresher o;
	 *   or 0 if they are equally fresh
	 */
	@Override
	public int compareTo(Job o) {
		long diff = lastAccessed - o.lastAccessed;
		if (diff == 0)
			return 0;
		return diff > 0 ? 1 : -1;
	}

	public long getLastAccessed() {
		return lastAccessed;
	}

	public void resetLastAccessed() {
		lastAccessed = System.currentTimeMillis();
	}

	public SearchParameters getParameters() {
		return par;
	}

	/** Perform the search.
	 *
	 * @param waitTimeMs if < 0, method blocks until the search is finished. For any
	 *   value >= 0, waits for the specified amount of time or until the search is finished,
	 *   then returns.
	 *
	 * @throws QueryException on parse error or other query-related error (e.g. too broad)
	 * @throws InterruptedException if the thread was interrupted
	 */
	final public void perform(int waitTimeMs) throws QueryException, InterruptedException {
		if (performCalled)
			throw new RuntimeException("Already performing search!");

		// Create and start thread
		// TODO: use thread pooling..?
		startedAt = System.currentTimeMillis();
		searchThread = new SearchThread(this);
		searchThread.start();
		performCalled = true;

		// Block until either...
		// * finished
		// * specified wait time elapsed (if waitTimeMs >= 0)
		int defaultWaitStep = 100;
		while (waitTimeMs != 0 && !searchThread.finished()) {
			int w = waitTimeMs < 0 ? defaultWaitStep : (waitTimeMs > defaultWaitStep ? defaultWaitStep : waitTimeMs);
			Thread.sleep(w);
			if (waitTimeMs >= 0)
				waitTimeMs -= w;
		}
	}

	@SuppressWarnings("unused")
	protected void performSearch() throws QueryException, IndexOpenException, InterruptedException {
		// (to override)
	}

	/**
	 * Is this search operation finished?
	 * (i.e. can we start working with the results?)
	 *
	 * @return true iff the search operation is finished and the results are available
	 */
	public boolean finished() {
		return performCalled && searchThread.finished();
	}

	/**
	 * Did the search throw an exception?
	 *
	 * @return true iff the search operation threw an exception
	 */
	public boolean threwException() {
		return finished() && searchThread.threwException();
	}

	/**
	 * Get the exception thrown by the search thread, if any
	 * @return the exception, or null if none was thrown
	 */
	public Throwable getThrownException() {
		return threwException() ? searchThread.getThrownException() : null;
	}

	/**
	 * Re-throw the exception thrown by the search thread, if any.

	 * @throws IndexOpenException
	 * @throws QueryException
	 * @throws InterruptedException
	 */
	public void rethrowException() throws IndexOpenException, QueryException, InterruptedException {
		Throwable exception = getThrownException();
		if (exception == null)
			return;
		if (exception instanceof IndexOpenException)
			throw (IndexOpenException)exception;
		else if (exception instanceof QueryException)
			throw (QueryException)exception;
		else if (exception instanceof InterruptedException)
			throw (InterruptedException)exception;
		throw ExUtil.wrapRuntimeException(exception);
	}

	/**
	 * Return this search's age in seconds.
	 *
	 * Age is defined as the time between now and the last time
	 * it was accessed.
	 *
	 * @return the age in seconds
	 */
	public int ageInSeconds() {
		return (int) (System.currentTimeMillis() - lastAccessed) / 1000;
	}

	/**
	 * How long this job took to execute (so far).
	 * @return execution time in ms
	 */
	public int executionTimeMillis() {
		if (startedAt < 0)
			return -1;
		if (finishedAt < 0)
			return (int)(System.currentTimeMillis() - startedAt);
		return (int)(finishedAt - startedAt);
	}

	/**
	 * Estimate how much memory this Search object holds.
	 *
	 * For now, defaults to an arbitrary 1M.
	 *
	 * TODO: implement size estimation in subclasses
	 *
	 * @return estimated memory size in bytes
	 */
	public long estimateSizeBytes() {
		return 1000000;
	}

	@Override
	public String toString() {
		return par.toString();
	}



}
