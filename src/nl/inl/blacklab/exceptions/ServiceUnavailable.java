package nl.inl.blacklab.exceptions;

import javax.servlet.http.HttpServletResponse;

public class ServiceUnavailable extends BlsException {
	
	public ServiceUnavailable(String msg) {
		super(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "SERVER_BUSY", msg);
	}

}
