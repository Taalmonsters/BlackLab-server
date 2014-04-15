package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectPlain;
import nl.inl.blacklab.server.search.IndexOpenException;

import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDoc extends RequestHandler {

	public RequestHandlerDoc(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException {
		logger.debug("REQ doc: " + indexName + "-" + urlPathInfo);

		Searcher searcher = searchMan.getSearcher(indexName);
		Document document = searcher.document(Integer.parseInt(urlPathInfo));
		String content = searcher.getContent(document);
		return new DataObjectPlain(content, "text/xml");
	}

}
