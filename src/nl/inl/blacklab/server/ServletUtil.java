package nl.inl.blacklab.server;

import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.util.ExUtil;

import org.apache.log4j.Logger;

public class ServletUtil {
	private static final Logger logger = Logger.getLogger(ServletUtil.class);

	/**
	 * Returns the value of a servlet parameter
	 * @param request the request object
	 * @param name
	 *            name of the parameter
	 * @return value of the paramater
	 */
	private static String getParameter(HttpServletRequest request, String name) {
		return request.getParameter(name);
	}

	/**
	 * Returns the value of a servlet parameter, or the default value
	 * @param request the request object
	 *
	 * @param name
	 *            name of the parameter
	 * @param defaultValue
	 *            default value
	 * @return value of the paramater
	 */
	public static int getParameter(HttpServletRequest request, String name, int defaultValue) {
		final String stringToParse = getParameter(request, name, "" + defaultValue);
		try {
			return Integer.parseInt(stringToParse);
		} catch (NumberFormatException e) {
			logger.info("Could not parse parameter '" + name + "', value '" + stringToParse
					+ "'. Using default (" + defaultValue + ")");
			return defaultValue;
		}
	}

	/**
	 * Returns the value of a servlet parameter, or the default value
	 * @param request the request object
	 *
	 * @param name
	 *            name of the parameter
	 * @param defaultValue
	 *            default value
	 * @return value of the paramater
	 */
	public static boolean getParameter(HttpServletRequest request, String name, boolean defaultValue) {
		String defStr = defaultValue ? "true" : "false";
		String value = getParameter(request, name, defStr);
		if (value.equalsIgnoreCase("true"))
			return true;
		if (value.equalsIgnoreCase("false"))
			return false;

		logger.warn("Illegal value '" + value + "' given for boolean parameter '" + name
				+ "'. Using default (" + defStr + ")");
		return defaultValue;
	}

	/**
	 * Returns the value of a servlet parameter, or the default value
	 * @param request the request object
	 * @param name
	 *            name of the parameter
	 * @param defaultValue
	 *            default value
	 * @return value of the paramater
	 */
	public static String getParameter(HttpServletRequest request, String name, String defaultValue) {
		String value = getParameter(request, name);
		if (value == null || value.length() == 0)
			value = defaultValue; // default action
		return value;
	}

	/**
	 * Returns the type of content the user would like as output (HTML, CSV, ...)
	 * This is based on the "outputformat" parameter.
	 *
	 * TODO: Also support HTTP Accept header!
	 *
	 * @param request the request object
	 * @return the type of content the user would like
	 */
	public static DataFormat getOutputType(HttpServletRequest request) {
		// See if we want non-HTML output (XML or CSV)
		String outputTypeString = getParameter(request, "outputformat", "").toLowerCase();
		if (outputTypeString.length() > 0) {
			return getOutputTypeFromString(outputTypeString);
		}
		return DataFormat.JSON;
	}

	/**
	 * Returns the desired content type for the output.
	 * This is based on the "outputformat" parameter.
	 * @param request the request object
	 * @return the MIME content type
	 */
	public static String getOutputContentType(HttpServletRequest request) {
		DataFormat outputType = getOutputType(request);
		if (outputType == DataFormat.XML)
			return "application/xml";
		return "application/json";
	}

	/**
	 * Translate the string value for outputType to the enum OutputType value.
	 *
	 * @param typeString
	 *            the outputType string
	 * @return the OutputType enum value
	 */
	static DataFormat getOutputTypeFromString(String typeString) {
		if (typeString.equalsIgnoreCase("xml"))
			return DataFormat.XML;
		if (typeString.equalsIgnoreCase("json"))
			return DataFormat.JSON;
		logger.warn("Onbekend outputtype gevraagd: " + typeString);
		return DataFormat.JSON;
	}

	/**
	 * Get a PrintStream for writing the response
	 * @param responseObject the response object
	 * @return the PrintStream
	 */
	public static PrintStream getPrintStream(HttpServletResponse responseObject) {
		try {
			return new PrintStream(responseObject.getOutputStream(), true, "utf-8");
		} catch (Exception e) {
			throw ExUtil.wrapRuntimeException(e);
		}
	}

	/** Output character encoding */
	static final String OUTPUT_ENCODING = "UTF-8";

	/** For how long returned pages are valid (30 minutes) */
	private static final int CACHE_TIME_SECONDS = 30 * 60;

	/** The HTTP date format, to use for the cache header */
	static DateFormat httpDateFormat;

	// Initialize the HTTP date format
	static {
		httpDateFormat = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
		httpDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}

	/**
	 * Write cache headers for the configured cache time.
	 * @param response the response object to write the headers to
	 */
	@SuppressWarnings("unused")
	private void setCacheHeaders(HttpServletResponse response) {
		GregorianCalendar cal = new GregorianCalendar();
		cal.add(Calendar.SECOND, CACHE_TIME_SECONDS);

		String expires;
		synchronized (httpDateFormat) {
			expires = httpDateFormat.format(cal.getTime());
		}

		// Output headers
		response.setHeader("Cache-Control", "PUBLIC, max-age=" + CACHE_TIME_SECONDS + ", must-revalidate");
		response.setHeader("Expires", expires);
	}

	/**
	 * Write the HTTP headers for the response, including caching, IE8 XSS protection (off!),
	 * encoding, save as file.
	 * @param request the request object
	 * @param response the response object
	 * @param noCache
	 *            if true, outputs headers to make sure the page isn't cached.
	 */
	@SuppressWarnings("unused")
	private void writeResponseHeaders(HttpServletRequest request, HttpServletResponse response) {
		boolean noCache = false;

		// For the login page: set the no-cache headers for the response,
		// so he doesn't see the login page again after the user has logged in.
		if (noCache) {
			response.setHeader("Pragma", "No-cache");
			response.setHeader("Cache-Control", "no-cache");
			response.setDateHeader("Expires", 1);
		}

		// Make sure IE8 doesn't do "smart" XSS prevention
		// (i.e. mangle our URLs and scripts for us)
		response.setIntHeader("X-XSS-Protection", 0);

		// Set the content headers for the response
		response.setCharacterEncoding(OUTPUT_ENCODING);
		response.setContentType(ServletUtil.getOutputContentType(request));
	}


}
