package nl.inl.blacklab.server.dataobject;

import java.io.IOException;
import java.io.Writer;

public class DataObjectNumber extends DataObject {

	double fNum;

	int iNum;

	boolean isInt;

	public DataObjectNumber(int iNum) {
		this.iNum = iNum;
		isInt = true;
	}

	public DataObjectNumber(double fNum) {
		this.fNum = fNum;
		isInt = false;
	}

	@Override
	public void serialize(Writer out, DataFormat fmt, boolean prettyPrint, int depth) throws IOException {
		if (isInt) {
			out.append("" + iNum);
		} else {
			out.append("" + fNum);
		}
	}

	@Override
	public boolean isSimple() {
		return true;
	}

}
