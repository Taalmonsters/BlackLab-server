package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;

import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDocInfo extends RequestHandler {

	public RequestHandlerDocInfo(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {

		int i = urlPathInfo.indexOf('/');
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");

		Searcher searcher = searchMan.getSearcher(indexName);
		int luceneDocId = searchMan.getLuceneDocIdFromPid(indexName, docId);
		Document document = searcher.document(luceneDocId);
		if (document == null)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");

		// Document info
		logger.debug("REQ doc info: " + indexName + "-" + docId);

		DataObjectMapElement response = new DataObjectMapElement();
		response.put("doc-pid", docId);
		response.put("doc-info", getDocumentInfo(indexName, searcher.getIndexStructure(), document));
		return response;
	}

}
