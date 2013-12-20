package nl.inl.blacklab.server;

import java.io.PrintStream;
import java.util.Map;

import javax.servlet.http.HttpServletResponse;

import nl.inl.blacklab.server.ServletUtil.OutputType;
import nl.inl.util.StringUtil;

public class ResponseDebug extends Response {

	private Map<String, String> info;

	public ResponseDebug(Map<String, String> info) {
		this.info = info;
	}

	@Override
	public void output(HttpServletResponse responseObject, OutputType outputType) {
		responseObject.setContentType(outputType == OutputType.XML ? "application/xml" : "application/json");
		StringBuilder response = new StringBuilder();
		switch(outputType) {
		case XML:
			response.append(XML_DECL + "<object>\n");
			break;
		default:
			response.append("{\n");
			break;
		}
		int n = 0;
		for (Map.Entry<String, String> e: info.entrySet()) {
			String value = e.getValue();
			if (value == null)
				value = "(null)";
			switch(outputType) {
			case XML:
				response
					.append("  <prop name=\"")
					.append(StringUtil.escapeXmlChars(e.getKey()))
					.append("\">")
					.append(StringUtil.escapeXmlChars(value))
					.append("</prop>\n");
				break;
			default:
				boolean isLast = n == info.size() - 1;
				n++;
				response
					.append("  \"")
					.append(StringUtil.escapeDoubleQuotedString(e.getKey()))
					.append("\": \"")
					.append(StringUtil.escapeDoubleQuotedString(value))
					.append("\"");
				if (!isLast)
					response.append(",");
				response.append("\n");
				break;
			}
		}
		switch(outputType) {
		case JSON:
			response.append("}\n");
			break;
		case XML:
			response.append("</object>\n");
			break;
		}
		PrintStream out = ServletUtil.getPrintStream(responseObject);
		out.println(response.toString());
	}
}
