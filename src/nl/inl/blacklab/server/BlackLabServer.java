package nl.inl.blacklab.server;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.requesthandlers.RequestHandler;

public class BlackLabServer extends HttpServlet {
	//private static final Logger logger = Logger.getLogger(BlackLabServer.class);

	/**
	 * Whether we're debugging or not. In debug mode, output is pretty printed by default.
	 */
	boolean isDebug = true;

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse responseObject) {
		try {
			// Handle the request
			DataObject response = RequestHandler.handle(this, request);

			// Output the response in the correct type
			responseObject.addHeader("Content-Type", ServletUtil.getOutputContentType(request));
			OutputStreamWriter out = new OutputStreamWriter(responseObject.getOutputStream(), "utf-8");
			boolean prettyPrint = ServletUtil.getParameter(request, "prettyprint", isDebug);
			response.serializeDocument("blacklab-response", out, ServletUtil.getOutputType(request), prettyPrint);
			out.flush();

		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}

	@Override
	public void destroy() {
		super.destroy();
	}

	@Override
	public String getServletInfo() {
		return "Provides corpus search services on one or more BlackLab indices.\n"
				+ "Source available at http://github.com/INL/\n"
				+ "(C) 2013,2014-... Instituut voor Nederlandse Lexicologie.\n"
				+ "Licensed under the Apache License.\n";
	}

	Map<String, Searcher> searchers = new HashMap<String, Searcher>();

	public synchronized Searcher getSearcher(String indexName) throws IOException {
		if (searchers.containsKey(indexName))  {
			return searchers.get(indexName);
		}
		File indexDir = getIndexDir(indexName);
		if (indexDir == null)  {
			throw new IOException("Index " + indexName + " not found");
		}
		Searcher searcher = Searcher.open(indexDir);
		searchers.put(indexName, searcher);
		return searcher;
	}

	private File getIndexDir(String indexName) {
		if (indexName.equals("opensonar")) {
			return new File("G:/Jan_OpenSonar/index");
		}
		return null;
	}

}
