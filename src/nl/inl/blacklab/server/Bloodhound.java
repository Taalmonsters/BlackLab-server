package nl.inl.blacklab.server;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class Bloodhound extends HttpServlet {
	//private static final Logger logger = Logger.getLogger(Bloodhound.class);

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse responseObject) {
		Response response = RequestHandler.handle(req);
		response.output(responseObject, ServletUtil.getOutputType(req));
	}

	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	}

	@Override
	public void destroy() {
		// TODO Auto-generated method stub
		super.destroy();
	}

	@Override
	public String getServletInfo() {
		return "Provides corpus search services on one or more BlackLab indices.\n"
				+ "Source available at http://github.com/INL/BlackLab/\n"
				+ "(C) 2013,2014-... Instituut voor Nederlandse Lexicologie.\n"
				+ "Licensed under the Apache License.\n";
	}

}
