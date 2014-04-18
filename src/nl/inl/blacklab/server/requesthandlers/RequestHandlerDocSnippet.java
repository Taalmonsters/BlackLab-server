package nl.inl.blacklab.server.requesthandlers;

import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Hit;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.Kwic;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectContextList;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.QueryException;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDocSnippet extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerDocSnippet.class);

	public RequestHandlerDocSnippet(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException {
		int i = urlPathInfo.indexOf('/');
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");
		debug(logger, "REQ doc contents: " + indexName + "-" + docId);

		int luceneDocId = searchMan.getLuceneDocIdFromPid(indexName, docId);
		Document document = searchMan.getSearcher(indexName).document(luceneDocId); //searchMan.getDocumentFromPid(indexName, docId);
		if (document == null)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");

		Searcher searcher = searchMan.getSearcher(indexName);
		int start = searchParam.getInteger("hitstart");
		int end = searchParam.getInteger("hitend");
		int wordsAroundHit = searchParam.getInteger("wordsaroundhit");
		if (wordsAroundHit > searchMan.getMaxSnippetSize()) {
			wordsAroundHit = searchMan.getMaxSnippetSize();
		}
		Hit hit = new Hit(luceneDocId, start, end);
		Hits hits = new Hits(searcher, Arrays.asList(hit));
		Kwic kwic = hits.getKwic(hit, wordsAroundHit);

		// Add KWIC info
		DataObjectMapElement doSnippet = new DataObjectMapElement();
		doSnippet.put("left", new DataObjectContextList(kwic.getProperties(), kwic.getLeft()));
		doSnippet.put("match", new DataObjectContextList(kwic.getProperties(), kwic.getMatch()));
		doSnippet.put("right", new DataObjectContextList(kwic.getProperties(), kwic.getRight()));

		return doSnippet;
	}

}
