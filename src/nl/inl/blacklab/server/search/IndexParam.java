package nl.inl.blacklab.server.search;

import java.io.File;

/** Index parameters */
class IndexParam {
	private File dir;

	private String pidField;

	private boolean mayViewContents;

	public IndexParam(File dir, String pidField, boolean mayViewContents) {
		super();
		this.dir = dir;
		this.pidField = pidField;
		this.mayViewContents = mayViewContents;
	}

	public File getDir() {
		return dir;
	}

	public String getPidField() {
		return pidField;
	}

	public boolean mayViewContents() {
		return mayViewContents;
	}

}