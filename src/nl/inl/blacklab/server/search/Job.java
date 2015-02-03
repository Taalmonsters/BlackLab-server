package nl.inl.blacklab.server.search;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;

import nl.inl.blacklab.perdocument.DocResults;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.exceptions.InternalServerError;
import nl.inl.blacklab.server.exceptions.ServiceUnavailable;
import nl.inl.util.ExUtil;
import nl.inl.util.ThreadPriority;
import nl.inl.util.ThreadPriority.Level;

import org.apache.log4j.Logger;

public abstract class Job implements Comparable<Job> {
	private static final int REFS_INVALID = -9999;

	protected static final Logger logger = Logger.getLogger(Job.class);
	
	/** If true (as it should be for production use), we call cleanup() on jobs that 
	 *  aren't referred to anymore in an effor to assist the Java garbage collector.
	 *  EXPERIMENTAL
	 */
	final static boolean ENABLE_JOB_CLEANUP = false;

	/** id for the next job started */
	static long nextJobId = 0;

	/**
	 * Create a new Search (subclass) object to carry out the specified search,
	 * and call the perform() method to start the search.
	 *
	 * @param searchMan the servlet
	 * @param user user creating the job
	 * @param par search parameters
	 * @return the new Search object
	 * @throws BlsException
	 */
	public static Job create(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		Job search = null;
		String jobClass = par.getString("jobclass");
		// TODO: use a map of String -> Class<? extends Job>
		if (jobClass.equals("JobHits")) {
			search = new JobHits(searchMan, user, par);
		} else if (jobClass.equals("JobDocs")) {
			search = new JobDocs(searchMan, user, par);
		} else if (jobClass.equals("JobHitsSorted")) {
			search = new JobHitsSorted(searchMan, user, par);
		} else if (jobClass.equals("JobDocsSorted")) {
			search = new JobDocsSorted(searchMan, user, par);
		} else if (jobClass.equals("JobHitsWindow")) {
			search = new JobHitsWindow(searchMan, user, par);
		} else if (jobClass.equals("JobDocsWindow")) {
			search = new JobDocsWindow(searchMan, user, par);
		} else if (jobClass.equals("JobHitsTotal")) {
			search = new JobHitsTotal(searchMan, user, par);
		} else if (jobClass.equals("JobDocsTotal")) {
			search = new JobDocsTotal(searchMan, user, par);
		} else if (jobClass.equals("JobHitsGrouped")) {
			search = new JobHitsGrouped(searchMan, user, par);
		} else if (jobClass.equals("JobDocsGrouped")) {
			search = new JobDocsGrouped(searchMan, user, par);
		} else
			throw new InternalServerError(1);
	
		return search;
	}

	/** Unique job id */
	long id = nextJobId++;
	
	/**
	 * Number of references to this Job. If this reaches 0, and the thread
	 * is not running, we can safely call cleanup().
	 * 
	 * Note that the cache itself also counts as a reference to the job,
	 * so if refsToJob == 1, it is only in the cache, not currently referenced
	 * by another job or search request. We can use this to decide when a
	 * search can safely be removed from the cache.
	 */
	int refsToJob = 0;
	
	/**
	 * The jobs we're waiting for, so we can notify them in case we get cancelled,
	 * and our "load scheduler" knows we're not currently using the CPU.
	 */
	Set<Job> waitingFor = new HashSet<Job>();

	/**
	 * Wait for the specified job to finish
	 * @param job the job to wait for
	 * @throws BlsException
	 */
	protected void waitForJobToFinish(Job job) throws BlsException {
		synchronized(waitingFor) {
			waitingFor.add(job);
			job.incrRef();
		}
		try {
			job.waitUntilFinished();
		} finally {
			synchronized(waitingFor) {
				job.decrRef();
				waitingFor.remove(job);
			}
		}
	}

	/** When this job was started (or -1 if not started yet) */
	protected long startedAt;

	/** When this job was finished (or -1 if not finished yet) */
	protected long finishedAt;
	
	/** If the search thread threw an exception, it's stored here. */
	protected Throwable thrownException;

	/** The last time the results of this search were accessed (for caching) */
	private long lastAccessed;

	/** If we're paused, this is the time when we were paused */
	private long pausedAt;

	/** The index searcher */
	protected Searcher searcher;

	/** Has perform() been called or not? Don't call it twice! */
	private boolean performCalled = false;

	/** Has cancelJob() been called or not? Don't call it twice! */
	private boolean cancelJobCalled = false;

	/** Thread object carrying out the search, if performing the search. */
	private SearchThread searchThread = null;

	/** Parameters uniquely identifying this search */
	protected SearchParameters par;

	/** The servlet */
	protected SearchManager searchMan;

	/** Who created this job? */
	protected User user;

	/** Is this job running in low priority? */
	protected ThreadPriority.Level level = ThreadPriority.Level.NORMAL;

	public Job(SearchManager searchMan, User user, SearchParameters par) throws BlsException {
		super();
		this.searchMan = searchMan;
		this.user = user;
		this.par = par;
		searcher = searchMan.getSearcher(par.getString("indexname"));
		resetLastAccessed();
		startedAt = -1;
		finishedAt = -1;
		thrownException = null;
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
	 * @throws BlsException on parse error or other query-related error (e.g. too broad)
	 */
	final public void perform(int waitTimeMs) throws BlsException {
		if (performCalled)
			throw new RuntimeException("Already performing search!");

		// Create and start thread
		// TODO: use thread pooling..?
		startedAt = System.currentTimeMillis();
		searchThread = new SearchThread(this);
		searchThread.start();
		performCalled = true;

		waitUntilFinished(waitTimeMs);
	}

	@SuppressWarnings("unused")
	protected void performSearch() throws BlsException {
		// (to override)
	}

	/**
	 * Is this search operation finished?
	 * (i.e. can we start working with the results?)
	 *
	 * @return true iff the search operation is finished and the results are available
	 */
	public boolean finished() {
		if (!performCalled)
			return false;
		return performCalled && (finishedAt >= 0 || thrownException != null);
	}

	/**
	 * Did the search throw an exception?
	 *
	 * @return true iff the search operation threw an exception
	 */
	public boolean threwException() {
		if (!performCalled)
			return false;
		return finished() && thrownException != null;
	}

	/**
	 * Get the exception thrown by the search thread, if any
	 * @return the exception, or null if none was thrown
	 */
	public Throwable getThrownException() {
		return threwException() ? thrownException : null;
	}

	/**
	 * Re-throw the exception thrown by the search thread, if any.

	 * @throws BlsException
	 */
	public void rethrowException() throws BlsException {
		Throwable exception = getThrownException();
		if (exception == null)
			return;
		logger.debug("Re-throwing exception from search thread:\n" + exception.getClass().getName() + ": " + exception.getMessage());
		if (exception instanceof BlsException)
			throw (BlsException)exception;
		throw ExUtil.wrapRuntimeException(exception);
	}

	/**
	 * Wait until this job's finished, an Exception is thrown or the specified
	 * time runs out.
	 *
	 * @param maxWaitMs maximum time to wait, or a negative number for no limit
	 * @throws BlsException
	 */
	public void waitUntilFinished(int maxWaitMs) throws BlsException {
		int defaultWaitStep = 100;
		boolean waitUntilFinished = maxWaitMs < 0;
		while (!performCalled || ( (waitUntilFinished || maxWaitMs > 0) && !finished())) {
			int w = defaultWaitStep;
			if (!waitUntilFinished) {
				if (maxWaitMs < defaultWaitStep)
					w = maxWaitMs;
				maxWaitMs -= w;
			}
			try {
				Thread.sleep(w);
			} catch (InterruptedException e) {
				throw new ServiceUnavailable("The server seems to be under heavy load right now. Please try again later.");
			}
		}
		// If an Exception occurred, re-throw it now.
		rethrowException();
	}

	/**
	 * Wait until this job is finished (or an Exception is thrown)
	 *
	 * @throws BlsException
	 */
	public void waitUntilFinished() throws BlsException {
		waitUntilFinished(-1);
	}

	/**
	 * Try to cancel this job.
	 */
	public void cancelJob() {
		if (!performCalled)
			return; // can't cancel, hasn't been started yet (shouldn't happen)
		if (cancelJobCalled)
			return; // don't call this twice!
		cancelJobCalled = true;
		
		searchThread.interrupt();
		searchThread = null; // ensure garbage collection

		// Tell the jobs we were waiting for we're no longer interested
		if (waitingFor != null) {
			synchronized(waitingFor) {
				for (Job j: waitingFor) {
					j.decrRef(); //decrementClientsWaiting();
				}
				waitingFor.clear();
			}
		}
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
		return user.uniqueIdShort();
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

	public DataObjectMapElement toDataObject(boolean debugInfo) {
		DataObjectMapElement stats = new DataObjectMapElement();
		boolean isCount = (this instanceof JobHitsTotal) || (this instanceof JobDocsTotal);
		stats.put("type", isCount ? "count" : "search");
		stats.put("status", status());
		stats.put("executionTime", executionTime());
		stats.put("notAccessedFor", notAccessedFor());
		stats.put("pausedFor", pausedFor());
		stats.put("createdBy", shortUserId());
		stats.put("refsToJob", refsToJob - 1); // (- 1 because the cache always references it)
		stats.put("waitingForJobs", waitingFor.size());
		
		DataObjectMapElement d = new DataObjectMapElement();
		d.put("id", id);
		d.put("class", getClass().getSimpleName());
		d.put("searchParam", par.toDataObject());
		d.put("stats", stats);
		
		if (debugInfo) {
			// Add extra debug info.
			DataObjectMapElement dbg = new DataObjectMapElement();
			
			// Ids of the jobs this thread is waiting for, if any
			DataObjectList wfIds = new DataObjectList("jobId");
			if (waitingFor.size() > 0) {
				for (Job j: waitingFor) {
					wfIds.add(j.id);
				}
			}
			dbg.put("waitingForIds", wfIds);
			
			// More information about job state
			dbg.put("startedAt", startedAt);
			dbg.put("finishedAt", finishedAt);
			dbg.put("lastAccessed", lastAccessed);
			dbg.put("pausedAt", pausedAt);
			dbg.put("performCalled", performCalled);
			dbg.put("cancelJobCalled", cancelJobCalled);
			dbg.put("priorityLevel", level.toString());
			dbg.put("resultsPriorityLevel", getPriorityOfResultsObject().toString());
			
			// Information about thrown exception, if any
			DataObjectMapElement ex = new DataObjectMapElement();
			if (thrownException != null) {
				PrintWriter st = new PrintWriter(new StringWriter());
				thrownException.printStackTrace(st);
				ex.put("class", thrownException.getClass().getName());
				ex.put("message", thrownException.getMessage());
				ex.put("stackTrace", st.toString());
			}
			dbg.put("thrownException", ex);
			
			// Information about thread object, if any
			DataObjectMapElement thr = new DataObjectMapElement();
			if (searchThread != null) {
				thr.put("name", searchThread.getName());
				thr.put("osPriority", searchThread.getPriority());
				thr.put("isAlive", searchThread.isAlive());
				thr.put("isDaemon", searchThread.isDaemon());
				thr.put("isInterrupted", searchThread.isInterrupted());
				thr.put("state", searchThread.getState().toString());
				StackTraceElement[] stackTrace = searchThread.getStackTrace();
				StringBuilder stackTraceStr = new StringBuilder();
				for (StackTraceElement element: stackTrace) {
					stackTraceStr.append(element.toString()).append("\n");
				}
				thr.put("currentlyExecuting", stackTraceStr.toString());
			}
			dbg.put("searchThread", thr);
			d.put("debugInfo", dbg);
		}

		return d;
	}
	
	private String status() {
		if (finished())
			return "finished";
		if (level == null)
			return "(level == NULL!)";
		switch(level) {
		case PAUSED: return "paused";
		case LOW:    return "lowprio";
		default: return "running";
		}
	}

	protected void cleanup() {
		logger.debug("Job.cleanup() called");
		if (waitingFor != null) {
			synchronized(waitingFor) {
				for (Job j: waitingFor) {
					j.decrRef();
				}
				waitingFor.clear();
				waitingFor = null;
			}
		}
		thrownException = null;
		searchThread = null;
		refsToJob = REFS_INVALID;
	}
	
	public synchronized void incrRef() {
		if (refsToJob == REFS_INVALID)
			throw new RuntimeException("Cannot add ref, job was already cleaned up!");
		refsToJob++;
	}

	public synchronized void decrRef() {
		refsToJob--;
		if (refsToJob == 1) {
			// Only in cache; set the last accessed time so we
			// know for how long it's been ignored.
			resetLastAccessed();
		} else if (refsToJob == 0) {
			// No references to this job, not even in the cache.
			// We can safely cancel it if it was still 
			// running. We optionally call cleanup to
			// assist with garbage collection.
			cancelJob();
			if (ENABLE_JOB_CLEANUP)
				cleanup();
		}
	}

	/**
	 * Return this search's cache age in seconds.
	 *
	 * Cache age is defined as the time between now and the last time
	 * it was accessed (for finished searches only).
	 * 
	 * Running searches always have a zero age. Check executionTime() 
	 * for search time.
	 *
	 * @return the age in seconds
	 */
	public double cacheAge() {
		if (finished())
			return (System.currentTimeMillis() - lastAccessed) / 1000.0;
		return 0;
	}

	/**
	 * How long this job took to execute (so far).
	 * 
	 * @return execution time in ms
	 */
	public double executionTime() {
		if (startedAt < 0)
			return -1;
		if (finishedAt < 0)
			return (System.currentTimeMillis() - startedAt) / 1000.0;
		return (finishedAt - startedAt) / 1000.0;
	}

	/**
	 * Returns how long ago this job was last accessed.
	 * 
	 * Note that if a client is waiting for this job to complete, this always returns 0.
	 * 
	 * @return how long ago this job was last accessed.
	 */
	public double notAccessedFor() {
		if (refsToJob > 1) {
			// More references to this job than just the cache;
			// This counts as being continually accessed, because
			// those jobs apparently need this job.
			return 0;
		}
		return (System.currentTimeMillis() - lastAccessed) / 1000.0;
	}

	/**
	 * How long has this job been paused for?
	 * @return number of ms since the job was paused, or 0 if not paused
	 */
	public double pausedFor() {
		if (level != Level.PAUSED)
			return 0;
		return (System.currentTimeMillis() - pausedAt) / 1000.0;
	}
	
	/**
	 * Is this job waiting for another job or jobs, and
	 * therefore not using the CPU?
	 * @return true if it's waiting, false if not
	 */
	public boolean isWaitingForOtherJob() {
		synchronized(waitingFor) {
			return waitingFor.size() > 0;
		}
	}

	/**
	 * Set the thread priority level.
	 * 
	 * @param level the desired priority level.
	 */
	public void setPriorityLevel(ThreadPriority.Level level) {
		if (this.level != level) {
			if (level == Level.PAUSED)
				pausedAt = System.currentTimeMillis();
			this.level = level;
		}
		setPriorityInternal();
	}

	/**
	 * Get the thread priority level.
	 * 
	 * @return the current priority level.
	 */
	public ThreadPriority.Level getPriorityLevel() {
		return level;
	}

	/**
	 * Set the operation to be normal priority, low priority or paused.
	 * 
	 * Depends on the lowPrio and paused variables.
	 */
	protected void setPriorityInternal() {
		// Subclasses can override this to set the priority of the operation
	}

	/**
	 * Get the actual priority of the Hits or DocResults object.
	 * @return the priority level
	 */
	public abstract Level getPriorityOfResultsObject();

	/**
	 * Set the priority/paused status of a Hits object.
	 * 
	 * @param h the Hits object
	 */
	protected void setHitsPriority(Hits h) {
		if (h != null) {
			h.setPriorityLevel(level);
		}
	}

	/**
	 * Set the priority/paused status of a DocResults object.
	 * 
	 * @param docResults the DocResults object
	 */
	protected void setDocsPriority(DocResults docResults) {
		if (docResults != null) {
			docResults.setPriorityLevel(level);
		}
	}

}
