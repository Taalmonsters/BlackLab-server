package nl.inl.blacklab.server.requesthandlers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.queryParser.corpusql.CorpusQueryLanguageParser;
import nl.inl.blacklab.queryParser.corpusql.ParseException;
import nl.inl.blacklab.search.Hit;
import nl.inl.blacklab.search.Hits;
import nl.inl.blacklab.search.Kwic;
import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.TextPattern;
import nl.inl.blacklab.server.ServletUtil;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectContextList;
import nl.inl.blacklab.server.dataobject.DataObjectList;
import nl.inl.blacklab.server.dataobject.DataObjectMap;

import org.apache.lucene.search.BooleanQuery.TooManyClauses;

public class RequestHandlerHitset extends RequestHandler {

	public RequestHandlerHitset(HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		String pattern = ServletUtil.getParameter(request, "patt", "");
		if (pattern.length() > 0) {
			int first = ServletUtil.getParameter(request, "first", 1);
			int number = ServletUtil.getParameter(request, "number", 50);
			Searcher searcher;
			try {
				searcher = servlet.getSearcher(indexName);
				TextPattern textPattern = CorpusQueryLanguageParser.parse(pattern);
				Hits hits = searcher.find(textPattern);
				DataObjectList hitList = new DataObjectList("hit");
				for (int i = first; i < first + number && i < hits.size(); i++) {
					Hit hit = hits.get(i);

					DataObjectMap hitMap = new DataObjectMap();
					hitMap.put("doc", hit.doc);
					hitMap.put("start", hit.start);
					hitMap.put("end", hit.end);

					Kwic c = hits.getKwic(hit);
					hitMap.put("left", new DataObjectContextList(c.properties, c.left));
					hitMap.put("match", new DataObjectContextList(c.properties, c.match));
					hitMap.put("right", new DataObjectContextList(c.properties, c.right));
					hitList.add(hitMap);
				}
				DataObjectMap response = new DataObjectMap();
				response.put("hits", hitList);
				return response;
			} catch (IOException e) {
				return RequestHandler.errorResponse("Unknown index " + indexName);
			} catch (TooManyClauses e) {
				return RequestHandler.errorResponse("Query too broad, too many matching terms");
			} catch (ParseException e) {
				return RequestHandler.errorResponse("Parse error in query: " + e.getMessage());
			}
		}
		return RequestHandler.errorResponse("No query given");
	}
}
