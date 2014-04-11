package nl.inl.blacklab.server.search;


/**
 * Thrown when the requested index was not available or could not be opened
 */
public class QueryException extends Exception {

	/** A symbolic error code that the client can recognize and show a custom message for. */
	private String errorCode;

	public QueryException() {
		super();
	}

	public QueryException(String code, String msg) {
		super(msg);
		this.errorCode = code;
	}

	public QueryException(Throwable cause) {
		super(cause);
	}

	public QueryException(String errorCode, String msg, Throwable cause) {
		super(msg, cause);
		this.errorCode = errorCode;
	}

	public String getErrorCode() {
		return errorCode;
	}
}
