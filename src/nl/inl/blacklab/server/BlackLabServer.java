package nl.inl.blacklab.server;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.requesthandlers.RequestHandler;
import nl.inl.blacklab.server.search.SearchManager;
import nl.inl.blacklab.server.search.SearchParameters;
import nl.inl.util.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class BlackLabServer extends HttpServlet {
	private static final Logger logger = Logger.getLogger(BlackLabServer.class);

	/** Whether we're debugging or not. In debug mode, output is pretty printed by default. */
	public static boolean DEBUG_MODE = true;

	/** Parameters involved in search */
	List<String> searchParameterNames;

	/** Manages all our searches */
	private SearchManager searchManager;

	@Override
	public void init() throws ServletException {
		// Default init if no log4j.properties found
		LogUtil.initLog4jIfNotAlready(Level.DEBUG);

		logger.info("Starting BlackLab Server...");

		super.init();

		// Keep a list of searchparameters.
		searchParameterNames = Arrays.asList(
				"resultsType", "patt", "pattlang", "pattfield", "filter", "filterlang",
				"sort", "group", "collator", "first", "number");

		searchManager = new SearchManager(DEBUG_MODE);

		logger.info("BlackLab Server ready.");

	}

	/**
	 * Process GET request (the only kind we accept)
	 *
	 * @param request HTTP request object
	 * @param responseObject where to write our response
	 */
	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse responseObject) {
		try {
			// Handle the request
			DataObject response = RequestHandler.handle(this, request);

			// Output the response in the correct type
			responseObject.addHeader("Content-Type", ServletUtil.getOutputContentType(request));
			OutputStreamWriter out = new OutputStreamWriter(responseObject.getOutputStream(), "utf-8");
			boolean prettyPrint = ServletUtil.getParameter(request, "prettyprint", DEBUG_MODE);
			response.serializeDocument("blacklab-response", out, ServletUtil.getOutputType(request), prettyPrint);
			out.flush();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void destroy() {
		super.destroy();
	}

	/**
	 * Provides a short description of this servlet.
	 * @return the description
	 */
	@Override
	public String getServletInfo() {
		return "Provides corpus search services on one or more BlackLab indices.\n"
				+ "Source available at http://github.com/INL/\n"
				+ "(C) 2013,2014-... Instituut voor Nederlandse Lexicologie.\n"
				+ "Licensed under the Apache License.\n";
	}

	/**
	 * Get the search-related parameteers from the request object.
	 *
	 * This ignores stuff like the requested output type, etc.
	 *
	 * Note also that the request type is not part of the SearchParameters, so from looking at these
	 * parameters alone, you can't always tell what type of search we're doing. The RequestHandler subclass
	 * will add a jobclass parameter when executing the actual search.
	 *
	 * @param request the HTTP request
	 * @param indexName the index to search
	 * @return the unique key
	 */
	public SearchParameters getSearchParameters(HttpServletRequest request, String indexName) {
		SearchParameters param = new SearchParameters();
		param.put("indexname", indexName);
		for (String name: searchParameterNames) {
			String value = ServletUtil.getParameter(request, name, searchManager.getParameterDefaultValue(name)).trim();
			if (value.length() == 0)
				continue;
			param.put(name, value);
		}
		return param;
	}

	public SearchManager getSearchManager() {
		return searchManager;
	}

}
