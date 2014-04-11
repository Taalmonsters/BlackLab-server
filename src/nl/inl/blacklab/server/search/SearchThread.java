package nl.inl.blacklab.server.search;

/**
 * A (background) thread the search is executed in.
 */
final class SearchThread extends Thread {
	/** The search to execute */
	private final Job search;

	/** If search execution failed, this is the exception that was thrown */
	Throwable thrownException = null;

	/**
	 * Construct a new SearchThread
	 * @param search the search to execute in the thread
	 */
	SearchThread(Job search) {
		this.search = search;
	}

	/**
	 * Run the thread, performing the requested search.
	 */
	@Override
	public void run() {
		try {
			search.performSearch();
			search.finishedAt = System.currentTimeMillis();
		} catch (Throwable e) {
			// NOTE: we catch Throwable here (while it's normally good practice to
			//  catch only Exception and derived classes) because we need to know if
			//  our thread crashed or not. The Throwable will be re-thrown by the
			//  main thread, so any non-Exception Throwables will then go uncaught
			//  as they "should".
			thrownException = e;
		}
	}

	/**
	 * Has the thread stopped running?
	 * @return true iff the thread has terminated
	 */
	public boolean finished() {
		State state = getState();
		return state == State.TERMINATED;
	}

	/**
	 * Did the thread throw an Exception?
	 * @return true iff it threw an Exception
	 */
	public boolean threwException() {
		return thrownException != null;
	}

	/**
	 * Get the Exception that was thrown by the thread (if any)
	 * @return the thrown Exception, or null if none was thrown
	 */
	public Throwable getThrownException() {
		return thrownException;
	}

}
