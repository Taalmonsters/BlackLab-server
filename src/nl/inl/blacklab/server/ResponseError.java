package nl.inl.blacklab.server;

import java.io.PrintStream;

import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.ServletUtil.OutputType;
import nl.inl.util.StringUtil;

public class ResponseError extends Response {

	private String message;

	public ResponseError(String message) {
		this.message = message;
	}

	@Override
	public void output(HttpServletResponse responseObject, OutputType outputType) {
		PrintStream out = ServletUtil.getPrintStream(responseObject);
		switch(outputType) {
		case JSON:
			out.println("{\"errorMessage\": \"" + StringUtil.escapeDoubleQuotedString(message) + "\"}");
			break;
		case XML:
			out.println(XML_DECL + "<errorMessage>" + StringUtil.escapeXmlChars(message) + "</errorMessage>");
			break;
		}
	}
}
