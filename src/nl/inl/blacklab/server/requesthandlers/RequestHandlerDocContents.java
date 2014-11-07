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
import nl.inl.blacklab.server.search.SearchManager;

import org.apache.lucene.document.Document;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerDocContents extends RequestHandler {
	public RequestHandlerDocContents(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException, QueryException, InterruptedException {
		int i = urlPathInfo.indexOf('/');
		String docId = i >= 0 ? urlPathInfo.substring(0, i) : urlPathInfo;
		if (docId.length() == 0)
			throw new QueryException("NO_DOC_ID", "Specify document pid.");

		Searcher searcher = getSearcher();
		DataFormat type = searchMan.getContentsFormat(indexName);
		int luceneDocId = SearchManager.getLuceneDocIdFromPid(searcher, docId);
		if (luceneDocId < 0)
			throw new QueryException("DOC_NOT_FOUND", "Document with pid '" + docId + "' not found.");
		Document document = searcher.document(luceneDocId); //searchMan.getDocumentFromPid(indexName, docId);
		if (document == null)
			throw new QueryException("INTERNAL_ERROR", "An internal error occurred. Please contact the administrator. Error code: 9.");
		if (!searcher.getIndexStructure().contentViewable()) {
			DataObject errObj = DataObject.errorObject("NOT_AUTHORIZED", "Sorry, you're not authorized to retrieve the full contents of this document.");
			errObj.overrideType(type); // Application expects this MIME type, don't disappoint
			return errObj;
		}

		String patt = searchParam.getString("patt");
		Hits hits = null;
		if (patt != null && patt.length() > 0) {
			//@@@ TODO: filter on document!
			searchParam.put("docpid", docId);
			JobWithHits search = searchMan.searchHits(user, searchParam);
			search.waitUntilFinished(SearchCache.MAX_SEARCH_TIME_SEC);
			if (!search.finished())
				return DataObject.errorObject("SEARCH_TIMED_OUT", "Search took too long, cancelled.");
			hits = search.getHits();
		}

		String content;
		int startAtWord = searchParam.getInteger("wordstart");
		int endAtWord = searchParam.getInteger("wordend");
		if (startAtWord < -1 || endAtWord < -1 || (startAtWord >= 0 && endAtWord >= 0 && endAtWord <= startAtWord) ) {
			throw new QueryException("ILLEGAL_BOUNDARIES", "Illegal word boundaries specified. Please check parameters.");
		}
		
		// Note: we use the highlighter regardless of whether there's hits because
		// it makes sure our document fragment is well-formed.
		Hits hitsInDoc = hits == null ? null : hits.getHitsInDoc(luceneDocId);
		content = searcher.highlightContent(luceneDocId, searcher.getMainContentsFieldName(), hitsInDoc, startAtWord, endAtWord);
		
		DataObjectPlain docContents = new DataObjectPlain(content, type);
		if (startAtWord == -1 && endAtWord == -1) {
			// Full document; no need for another root element
			docContents.setAddRootElement(false); // don't add another root element
		}
		return docContents;
	}
}
