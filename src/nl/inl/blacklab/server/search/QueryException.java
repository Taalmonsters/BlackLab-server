package nl.inl.blacklab.server.search;

import nl.inl.blacklab.server.ServletUtil;


/**
 * Thrown when the requested index was not available or could not be opened
 */
public class QueryException extends Exception {

	/** A symbolic error code that the client can recognize and show a custom message for. */
	private String errorCode;
	
	public static QueryException internalError(Exception e, boolean debugMode, int code) {
		return new QueryException("INTERNAL_ERROR", ServletUtil.internalErrorMessage(e, debugMode, code));
	}

	public static QueryException internalError(String message, boolean debugMode, int code) {
		return new QueryException("INTERNAL_ERROR", ServletUtil.internalErrorMessage(message, debugMode, code));
	}

	public static QueryException internalError(int code) {
		return new QueryException("INTERNAL_ERROR", ServletUtil.internalErrorMessage(code));
	}

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
