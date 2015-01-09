package nl.inl.blacklab.exceptions;


public class IndexNotFound extends NotFound {
	
	public IndexNotFound(String indexName) {
		super("CANNOT_OPEN_INDEX", "Could not open index '" + indexName + "'. Please check the name.");
	}

}
