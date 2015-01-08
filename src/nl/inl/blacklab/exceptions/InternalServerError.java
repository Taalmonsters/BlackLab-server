package nl.inl.blacklab.exceptions;

import javax.servlet.http.HttpServletResponse;

public class InternalServerError extends BlsException {
	
	private int internalErrorCode;

	public int getInternalErrorCode() {
		return internalErrorCode;
	}

	public InternalServerError(int code) {
		this("Internal error", code, null);
	}

	public InternalServerError(String msg, int internalErrorCode) {
		this(msg, internalErrorCode, null);
	}

	public InternalServerError(String msg, int internalErrorCode, Throwable cause) {
		super(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "INTERNAL_ERROR", msg, cause);
		this.internalErrorCode = internalErrorCode;
	}

}
