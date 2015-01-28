package nl.inl.blacklab.server.exceptions;

import nl.inl.blacklab.server.requesthandlers.Response;

import org.apache.log4j.Logger;

public class ConfigurationException extends InternalServerError {
	static final Logger logger = Logger.getLogger(Response.class);
	
	public ConfigurationException() {
		super("Configuration exception", 29, null);
	}

	public ConfigurationException(String msg) {
		super(msg, 29, null);
	}

	public ConfigurationException(String msg, Throwable cause) {
		super(msg + (cause == null ? "" : " (" + cause + ")"), 29, cause);
	}

}
