package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.dataobject.DataObjectPlain;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;

import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDoc extends RequestHandler {

	public RequestHandlerDoc(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {

		int i = urlPathInfo.indexOf('/');
		String subOperation = "";
		if (i >= 0)
			subOperation = urlPathInfo.substring(i + 1);
		if (subOperation.endsWith("/"))
			subOperation = subOperation.substring(0, subOperation.length() - 1);
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");

		Searcher searcher = searchMan.getSearcher(indexName);
		Document document = searchMan.getDocumentFromPid(indexName, docId);
		if (document == null)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");

		if (subOperation.equals("contents")) {
			// Document contents
			logger.debug("REQ doc contents: " + indexName + "-" + docId);

			DataFormat type = searchMan.getContentsFormat(indexName);
			if (searchMan.mayViewContents(indexName, document)) {
				String content = searcher.getContent(document);
				return new DataObjectPlain(content, type);
			}
			DataObject errObj = DataObject.errorObject("NOT_AUTHORIZED", "Sorry, you're not authorized to retrieve the full contents of this document.");
			errObj.overrideType(type); // Application expects this MIME type, don't disappoint
			return errObj;
		}

		if (subOperation.length() == 0) {
			// Document info
			logger.debug("REQ doc info: " + indexName + "-" + docId);

			DataObjectMapElement response = new DataObjectMapElement();
			response.put("doc-pid", docId);
			response.put("doc-info", getDocumentInfo(indexName, searcher.getIndexStructure(), document));
			return response;
		}

		throw new QueryException("UNKNOWN_DOC_OPERATION", "Unknown document operation: '" + subOperation + "'");
	}

}
