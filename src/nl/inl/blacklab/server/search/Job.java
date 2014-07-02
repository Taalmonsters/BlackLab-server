package nl.inl.blacklab.server.search;

import java.util.HashSet;
import java.util.Set;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.util.ExUtil;

import org.apache.log4j.Logger;

public abstract class Job implements Comparable<Job> {
	protected static final Logger logger = Logger.getLogger(Job.class);

	/** id for the next job started */
	static long nextJobId = 0;

	/** Unique job id */
	long id = nextJobId++;

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
	 * @throws QueryException
	 * @throws IndexOpenException
	 */
	protected void waitForJobToFinish(Job job) throws InterruptedException, IndexOpenException, QueryException {
		waitingFor.add(job);
		job.waitUntilFinished();
		waitingFor.remove(job);
	}

	/**
	 * Create a new Search (subclass) object to carry out the specified search,
	 * and call the perform() method to start the search.
	 *
	 * @param searchMan the servlet
	 * @param userId user creating the job
	 * @param par search parameters
	 * @return the new Search object
	 * @throws IndexOpenException
	 * @throws QueryException
	 */
	public static Job create(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException, QueryException {
		Job search = null;
		String jobClass = par.get("jobclass");
		// TODO: use a map of String -> Class<? extends Job>
		if (jobClass.equals("JobHits")) {
			search = new JobHits(searchMan, userId, par);
		} else if (jobClass.equals("JobDocs")) {
			search = new JobDocs(searchMan, userId, par);
		} else if (jobClass.equals("JobHitsSorted")) {
			search = new JobHitsSorted(searchMan, userId, par);
		} else if (jobClass.equals("JobDocsSorted")) {
			search = new JobDocsSorted(searchMan, userId, par);
		} else if (jobClass.equals("JobHitsWindow")) {
			search = new JobHitsWindow(searchMan, userId, par);
		} else if (jobClass.equals("JobDocsWindow")) {
			search = new JobDocsWindow(searchMan, userId, par);
		} else if (jobClass.equals("JobHitsTotal")) {
			search = new JobHitsTotal(searchMan, userId, par);
		} else if (jobClass.equals("JobDocsTotal")) {
			search = new JobDocsTotal(searchMan, userId, par);
		} else if (jobClass.equals("JobHitsGrouped")) {
			search = new JobHitsGrouped(searchMan, userId, par);
		} else if (jobClass.equals("JobDocsGrouped")) {
			search = new JobDocsGrouped(searchMan, userId, par);
		} else
			throw new QueryException("INTERNAL_ERROR", "An internal error occurred. Please contact the administrator. Error code: 1.");

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

	/** Who created this job? */
	protected String userId;

	public Job(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super();
		this.searchMan = searchMan;
		this.userId = userId;
		this.par = par;
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
	 * @throws IndexOpenException if the index couldn't be opened
	 */
	final public void perform(int waitTimeMs) throws QueryException, InterruptedException, IndexOpenException {
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
		logger.debug("Re-throwing exception from search thread:\n" + exception.getClass().getName() + ": " + exception.getMessage());
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
	 * @throws QueryException
	 * @throws IndexOpenException
	 */
	public void waitUntilFinished(int maxWaitMs) throws InterruptedException, IndexOpenException, QueryException {
		int defaultWaitStep = 100;
		while (maxWaitMs != 0 && !searchThread.finished()) {
			int w = maxWaitMs < 0 ? defaultWaitStep : (maxWaitMs > defaultWaitStep ? defaultWaitStep : maxWaitMs);
			Thread.sleep(w);
			if (maxWaitMs >= 0)
				maxWaitMs -= w;
		}
		// If an Exception occurred, re-throw it now.
		rethrowException();
	}

	/**
	 * Wait until this job is finished (or an Exception is thrown)
	 *
	 * @throws InterruptedException
	 * @throws QueryException
	 * @throws IndexOpenException
	 */
	public void waitUntilFinished() throws InterruptedException, IndexOpenException, QueryException {
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
			error(logger, "clientsWaiting < 0 for job: " + this);
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
		return id + ": " + par.toString();
	}

	private String shortUserId() {
		return userId.substring(0, 6);
	}

	public void debug(Logger logger, String msg) {
		logger.debug(shortUserId() + " " + msg);
	}

	public void warn(Logger logger, String msg) {
		logger.warn(shortUserId() + " " + msg);
	}

	public void info(Logger logger, String msg) {
		logger.info(shortUserId() + " " + msg);
	}

	public void error(Logger logger, String msg) {
		logger.error(shortUserId() + " " + msg);
	}

	public DataObjectMapElement toDataObject() {
		DataObjectMapElement stats = new DataObjectMapElement();
		stats.put("clients-waiting", clientsWaiting);
		stats.put("waiting-for-jobs", waitingFor.size());
		stats.put("started-at", (startedAt - searchMan.createdAt)/1000.0);
		stats.put("finished-at", (finishedAt - searchMan.createdAt)/1000.0);
		stats.put("last-accessed", (lastAccessed - searchMan.createdAt)/1000.0);
		stats.put("created-by", shortUserId());
		stats.put("thread-finished", searchThread.finished());

		DataObjectMapElement d = new DataObjectMapElement();
		d.put("id", id);
		d.put("class", getClass().getSimpleName());
		d.put("search-param", par.toDataObject());
		d.put("stats", stats);
		return d;
	}

}
