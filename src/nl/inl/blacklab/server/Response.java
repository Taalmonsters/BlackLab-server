package nl.inl.blacklab.server;

import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.ServletUtil.OutputType;

public abstract class Response {

	final static String XML_DECL = "<?xml version=\"1.0\" encoding=\"utf-8\" ?>\n";

	public abstract void output(HttpServletResponse responseObject, OutputType outputType);
}
