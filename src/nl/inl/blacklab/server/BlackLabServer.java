package nl.inl.blacklab.server;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.requesthandlers.RequestHandler;
import nl.inl.blacklab.server.search.SearchManager;
import nl.inl.blacklab.server.search.SearchParameters;
import nl.inl.util.IoUtil;
import nl.inl.util.LogUtil;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class BlackLabServer extends HttpServlet {
	private static final Logger logger = Logger.getLogger(BlackLabServer.class);

	/** Manages all our searches */
	private SearchManager searchManager;

	@Override
	public void init() throws ServletException {
		// Default init if no log4j.properties found
		LogUtil.initLog4jIfNotAlready(Level.DEBUG);

		logger.info("Starting BlackLab Server...");

		super.init();

		// Read JSON config file
		String configFileName = "blacklab-server.json";
		File configFile = new File(getServletContext().getRealPath("/../" + configFileName));
		InputStream is;
		if (configFile.exists()) {
			// Read from webapps dir
			try {
				logger.debug("Reading configuration file " + configFile);
				is = new BufferedInputStream(new FileInputStream(configFile));
			} catch (FileNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			// Read from classpath
			is = getClass().getClassLoader().getResourceAsStream(configFileName);
			if (is == null) {
				configFileName = "blacklab-server-defaults.json"; // internal defaults file
				is = getClass().getClassLoader().getResourceAsStream(configFileName);
				if (is == null)
					throw new ServletException("Could not find " + configFileName + "!");
			}
			logger.debug("Reading configuration file from classpath: " + configFileName);
		}
		JSONObject config;
		try {
			config = new JSONObject(readFileStripLineComments(is));
		} catch (Exception e) {
			throw new ServletException("Error reading JSON config file", e);
		}

//		Properties properties = new Properties();
//		if (DEBUG_MODE) {
//			properties.setProperty("debugMode", "true");
//
//			// Some test indices
//			properties.put("index.brown",     "D:/dev/blacklab/brown/index");
//			properties.put("index.brown.may-view-content", "true");
//
//			properties.put("index.opensonar", "D:/dev/blacklab/opensonar/index");
//			properties.put("index.opensonar.pid", "id");
//			properties.put("index.opensonar.may-view-content", "true");
//
//			properties.put("index.gysseling", "D:/dev/blacklab/gysseling/index");
//			properties.put("index.gysseling.pid", "idno");
//			properties.put("index.gysseling.may-view-content", "true");
//		}

		searchManager = new SearchManager(config);

		logger.info("BlackLab Server ready.");

	}

	public static String readFileStripLineComments(InputStream is) throws IOException {
		BufferedReader reader = IoUtil.makeBuffered(new InputStreamReader(is));
		StringBuilder b = new StringBuilder();
		while (true) {
			String line = reader.readLine();
			if (line == null)
				break;
			line = line.replaceAll("//.+$", "").trim();
			b.append(line).append("\n");
		}
		return b.toString();
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
			boolean debugMode = searchManager.isDebugMode(request.getRemoteAddr());
			DataObject response = RequestHandler.handle(this, request, debugMode);

			// Determine response type
			DataFormat outputType = response.getOverrideType(); // some responses override the user's request (i.e. article XML)
			if (outputType == null)
				outputType = ServletUtil.getOutputType(request, searchManager.getDefaultOutputType());

			// Write HTTP headers (content type and cache)
			responseObject.setCharacterEncoding("utf-8");
			responseObject.setContentType(ServletUtil.getContentType(outputType));
			ServletUtil.writeCacheHeaders(responseObject, searchManager.getClientCacheTimeSec());

			// Write the response
			OutputStreamWriter out = new OutputStreamWriter(responseObject.getOutputStream(), "utf-8");
			boolean prettyPrint = ServletUtil.getParameter(request, "prettyprint", debugMode);
			response.serializeDocument("blacklab-response", out, outputType, prettyPrint);
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
		for (String name: searchManager.getSearchParameterNames()) {
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
