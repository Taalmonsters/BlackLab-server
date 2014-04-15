package nl.inl.blacklab.server.dataobject;

import java.io.IOException;
import java.io.Writer;

/**
 * A plain value that be output as-is, not converted into JSON or XML.
 */
public class DataObjectPlain extends DataObject {

	String value;

	String mimeType;

	public DataObjectPlain(String value, String mimeType) {
		this.value = value;
		this.mimeType = mimeType;
	}

	public DataObjectPlain(String value) {
		this(value, "text/plain");
	}

	public String getMimeType() {
		return mimeType;
	}

	@Override
	public void serialize(Writer out, DataFormat fmt, boolean prettyPrint, int depth) throws IOException {
		out.append(value);
	}

	@Override
	public boolean isSimple() {
		return true;
	}

}
