package nl.inl.blacklab.server.dataobject;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import nl.inl.util.StringUtil;

public class DataObjectContextList extends DataObject {

	List<String> names = new ArrayList<String>();

	List<String> values = new ArrayList<String>();

	public DataObjectContextList(List<String> names, List<String> values) {
		this.names = names;
		this.values = values;
	}

	@Override
	public void serialize(Writer out, DataFormat fmt, boolean prettyPrint, int depth) throws IOException {
		switch (fmt) {
		case JSON:
			out.append("[");
			break;
		case XML:
			break;
		}
		boolean first = true;
		depth++;
		int valuesPerWord = names.size();
		int numberOfWords = values.size() / valuesPerWord;
		for (int i = 0; i < numberOfWords; i++) {
			int vIndex = i * valuesPerWord;
			int j = 0;
			switch (fmt) {
			case JSON:
				if (!first)
					out.append(",");
				if (prettyPrint) {
					out.append("\n");
					indent(out, depth);
				}
				out.append("{\"punct\":\"").append(StringUtil.escapeDoubleQuotedString(values.get(vIndex))).append("\"");
				for (int k = 1; k < names.size() - 1; k++) {
					String name = names.get(k);
					String value = values.get(vIndex + 1 + j);
					out.append(",\"").append(name).append("\":\"").append(StringUtil.escapeDoubleQuotedString(value)).append("\"");
					j++;
				}
				out.append(",\"word\":\"").append(StringUtil.escapeDoubleQuotedString(values.get(vIndex + 1 + j))).append("\"}");
				break;
			case XML:
				if (prettyPrint)
					indent(out, depth);
				out.append(StringUtil.escapeXmlChars(values.get(vIndex)));
				out.append("<w");
				for (int k = 1; k < names.size() - 1; k++) {
					String name = names.get(k);
					String value = values.get(vIndex + 1 + j);
					out.append(" ").append(name).append("=\"").append(StringUtil.escapeXmlChars(value)).append("\"");
					j++;
				}
				out.append(">");
				out.append(StringUtil.escapeXmlChars(values.get(vIndex + 1 + j)));
				out.append("</w>");
				if (prettyPrint)
					out.append("\n");
				break;
			}
			first = false;
		}
		depth--;
		switch (fmt) {
		case JSON:
			if (prettyPrint) {
				out.append("\n");
				indent(out, depth);
			}
			out.append("]");
			break;
		case XML:
			break;
		}
	}

	@Override
	public boolean isSimple() {
		return false;
	}


}
