package nl.inl.blacklab.server.dataobject;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public abstract class DataObject {

	public abstract void serialize(Writer out, DataFormat format, boolean prettyPrint, int depth) throws IOException;

	public void serialize(Writer out, DataFormat format, boolean prettyPrint) throws IOException {
		serialize(out, format, prettyPrint, 0);
	}

	public void serialize(Writer out, DataFormat format) throws IOException {
		serialize(out, format, false, 0);
	}

	public String toString(DataFormat fmt) {
		StringWriter sw = new StringWriter();
		try {
			this.serialize(sw, fmt, true);
			return sw.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String toString() {
		return toString(DataFormat.JSON);
	}

	void indent(Writer out, int depth) throws IOException {
		for (int i = 0; i < depth; i++) {
			out.append("  ");
		}
	}

	public abstract boolean isSimple();

	public void serializeDocument(String rootElementName, Writer out, DataFormat outputType, boolean prettyPrint) throws IOException {
		switch (outputType) {
		case JSON:
			break;
		case XML:
			out.append("<?xml version=\"1.0\" encoding=\"utf-8\" ?>");
			if (prettyPrint)
				out.append("\n");
			out.append("<").append(rootElementName).append(">");
			if (prettyPrint)
				out.append("\n");
			break;
		}
		serialize(out, outputType, prettyPrint, outputType == DataFormat.XML ? 1 : 0);
		switch (outputType) {
		case JSON:
			break;
		case XML:
			out.append("</").append(rootElementName).append(">");
			if (prettyPrint)
				out.append("\n");
			break;
		}
	}

}
