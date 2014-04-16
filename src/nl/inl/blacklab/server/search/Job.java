package nl.inl.blacklab.server.search;

import java.util.HashSet;
import java.util.Set;

import nl.inl.blacklab.search.Searcher;
import nl.inl.util.ExUtil;

import org.apache.log4j.Logger;

public abstract class Job implements Comparable<Job> {

	/** Log4j logger object */
	protected static final Logger logger = Logger.getLogger(JobHits.class);

	/** Number of clients waiting for the results of this job.
	 * This is used to allow clients to cancel long searches: if this number reaches
	 * 0 before the search is done, it may be cancelled. Jobs that use other Jobs will
	 * also count as a client of that Job, and will tell that Job they're no longer interested
	 * if they are cancelled themselves.
	 */
	int clientsWaiting = 0;

	/**
	 * The jobs we're waiting for, so we can notify them in case we get cancelled.
	 */
	Set<Job> waitingFor = new HashSet<Job>();

	/**
	 * Add a job we're waiting for.
	 * @param j the job
	 */
	protected void addToWaitingFor(Job j) {
		waitingFor.add(j);
	}

	/**
	 * Add a job we're waiting for.
	 * @param j the job
	 */
	protected void removeFromWaitingFor(Job j) {
		waitingFor.remove(j);
	}

	/**
	 * Wait for the specified job to finish
	 * @param job the job to wait for
	 * @throws InterruptedException
	 */
	protected void waitForJobToFinish(Job job) throws InterruptedException {
		waitingFor.add(job);
		job.waitUntilFinished();
		waitingFor.remove(job);
	}

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
		} else if (jobClass.equals("JobDocs")) {
			search = new JobDocs(searchMan, par);
		} else if (jobClass.equals("JobHitsSorted")) {
			search = new JobHitsSorted(searchMan, par);
		} else if (jobClass.equals("JobDocsSorted")) {
			search = new JobDocsSorted(searchMan, par);
		} else if (jobClass.equals("JobHitsWindow")) {
			search = new JobHitsWindow(searchMan, par);
		} else if (jobClass.equals("JobDocsWindow")) {
			search = new JobDocsWindow(searchMan, par);
		} else if (jobClass.equals("JobHitsTotal")) {
			search = new JobHitsTotal(searchMan, par);
		} else if (jobClass.equals("JobDocsTotal")) {
			search = new JobDocsTotal(searchMan, par);
		} else if (jobClass.equals("JobHitsGrouped")) {
			search = new JobHitsGrouped(searchMan, par);
		} else if (jobClass.equals("JobDocsGrouped")) {
			search = new JobDocsGrouped(searchMan, par);
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
		clientsWaiting++; // someone wants to know the answer

		waitUntilFinished(waitTimeMs);
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
	 * it was accessed, but only for finished searches. Running
	 * searches always have a zero age. Check executionTimeMillis() for
	 * search time.
	 *
	 * @return the age in seconds
	 */
	public int ageInSeconds() {
		if (finished())
			return (int) (System.currentTimeMillis() - lastAccessed) / 1000;
		return 0;
	}

	/**
	 * Wait until this job's finished, an Exception is thrown or the specified
	 * time runs out.
	 *
	 * @param maxWaitMs maximum time to wait, or a negative number for no limit
	 * @throws InterruptedException if the thread was interrupted
	 */
	public void waitUntilFinished(int maxWaitMs) throws InterruptedException {
		int defaultWaitStep = 100;
		while (maxWaitMs != 0 && !searchThread.finished()) {
			int w = maxWaitMs < 0 ? defaultWaitStep : (maxWaitMs > defaultWaitStep ? defaultWaitStep : maxWaitMs);
			Thread.sleep(w);
			if (maxWaitMs >= 0)
				maxWaitMs -= w;
		}
	}

	/**
	 * Wait until this job is finished (or an Exception is thrown)
	 *
	 * @throws InterruptedException
	 */
	public void waitUntilFinished() throws InterruptedException {
		waitUntilFinished(-1);
	}

	/**
	 * Should this job be cancelled?
	 *
	 * True if the job hasn't finished and there are no more clients
	 * waiting for its results.
	 *
	 * @return true iff the job should be cancelled
	 */
	public boolean shouldBeCancelled() {
		return !finished() && clientsWaiting == 0;
	}

	/**
	 * Change how many clients are waiting for the results of this job.
	 * @param delta how many clients to add or subtract
	 */
	public void changeClientsWaiting(int delta) {
		clientsWaiting += delta;
		if (clientsWaiting < 0)
			logger.error("clientsWaiting < 0 for job: " + this);
		if (shouldBeCancelled()) {
			cancelJob();
		}
	}

	/**
	 * Try to cancel this job.
	 */
	public void cancelJob() {
		searchThread.interrupt();

		// Tell the jobs we were waiting for we're no longer interested
		for (Job j: waitingFor) {
			j.changeClientsWaiting(-1);
		}
		waitingFor.clear();
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
