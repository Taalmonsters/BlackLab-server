package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchManager;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDocInfo extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerDocInfo.class);

	public RequestHandlerDocInfo(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {

		int i = urlPathInfo.indexOf('/');
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");

		Searcher searcher = getSearcher();
		int luceneDocId = SearchManager.getLuceneDocIdFromPid(searcher, docId);
		if (luceneDocId < 0)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");
		Document document = searcher.document(luceneDocId);
		if (document == null)
			throw new QueryException("INTERNAL_ERROR", "An internal error occurred. Please contact the administrator. Error code: 8.");

		// Document info
		debug(logger, "REQ doc info: " + indexName + "-" + docId);

		DataObjectMapElement response = new DataObjectMapElement();
		response.put("docPid", docId);
		response.put("docInfo", getDocumentInfo(searcher, document));
		return response;
	}

}
