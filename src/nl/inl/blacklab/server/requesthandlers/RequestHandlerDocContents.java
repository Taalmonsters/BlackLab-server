package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataFormat;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectPlain;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.blacklab.server.search.JobWithHits;
import nl.inl.blacklab.server.search.QueryException;
import nl.inl.blacklab.server.search.SearchCache;

import org.apache.log4j.Logger;
import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDocContents extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerDocContents.class);

	public RequestHandlerDocContents(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {
		int i = urlPathInfo.indexOf('/');
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");
		debug(logger, "REQ doc contents: " + indexName + "-" + docId);

		DataFormat type = searchMan.getContentsFormat(indexName);
		int luceneDocId = searchMan.getLuceneDocIdFromPid(indexName, docId);
		if (luceneDocId < 0)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");
		Document document = searchMan.getSearcher(indexName).document(luceneDocId); //searchMan.getDocumentFromPid(indexName, docId);
		if (document == null)
			throw new QueryException("INTERNAL_ERROR", "An internal error occurred. Please contact the administrator. Error code: 8.");
		if (!searchMan.mayViewContents(indexName, document)) {
			DataObject errObj = DataObject.errorObject("NOT_AUTHORIZED", "Sorry, you're not authorized to retrieve the full contents of this document.");
			errObj.overrideType(type); // Application expects this MIME type, don't disappoint
			return errObj;
		}

		String patt = searchParam.getString("patt");
		Hits hits = null;
		if (patt != null && patt.length() > 0) {
			//@@@ TODO: filter on document!
			searchParam.put("docPid", docId);
			JobWithHits search = searchMan.searchHits(getUserId(), searchParam);
			search.waitUntilFinished(SearchCache.MAX_SEARCH_TIME_SEC);
			if (!search.finished())
				return DataObject.errorObject("SEARCH_TIMED_OUT", "Search took too long, cancelled.");
			hits = search.getHits();
		}

		Searcher searcher = searchMan.getSearcher(indexName);
		String content;
		if (hits == null)
			content = searcher.getContent(document);
		else {
			content = searcher.highlightContent(luceneDocId, hits.getHitsInDoc(luceneDocId));
		}
		return new DataObjectPlain(content, type);
	}

}
