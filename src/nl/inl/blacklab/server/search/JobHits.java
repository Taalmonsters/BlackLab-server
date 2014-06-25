package nl.inl.blacklab.server.search;


import nl.inl.blacklab.search.SingleDocIdFilter;
import nl.inl.blacklab.search.TextPattern;

import org.apache.log4j.Logger;
import org.apache.lucene.search.BooleanQuery.TooManyClauses;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;

/**
 * Represents a hit search operation.
 */
public class JobHits extends JobWithHits {
	@SuppressWarnings("hiding")
	protected static final Logger logger = Logger.getLogger(JobHits.class);

	/** The parsed pattern */
	protected TextPattern textPattern;

	/** The parsed filter */
	protected Filter filterQuery;

	public JobHits(SearchManager searchMan, String userId, SearchParameters par) throws IndexOpenException {
		super(searchMan, userId, par);
	}

	@Override
	public void performSearch() throws QueryException, IndexOpenException {
		try {
			textPattern = searchMan.parsePatt(par.get("indexname"), par.get("patt"), par.get("pattlang"));
			Query q;
			if (par.get("doc-pid") != null) {
				// Only hits in 1 doc (for highlighting)
				int luceneDocId = searchMan.getLuceneDocIdFromPid(par.get("indexname"), par.get("doc-pid"));
				filterQuery = new SingleDocIdFilter(luceneDocId);
				debug(logger, "Filtering on single doc-id");
			} else {
				// Filter query
				q = SearchManager.parseFilter(par.get("filter"), par.get("filterlang"));
				filterQuery = q == null ? null : new QueryWrapperFilter(q);
			}
			try {
				hits = searcher.find(textPattern, filterQuery);
			} catch (RuntimeException e) {
				// TODO: catch a more specific exception!
				throw new QueryException("SEARCH_ERROR", "Search error: " + e.getMessage());
			}
		} catch (TooManyClauses e) {
			throw new QueryException("QUERY_TOO_BROAD", "Query too broad, too many matching terms. Please be more specific.", e);
		}
	}

	public TextPattern getTextPattern() {
		return textPattern;
	}

	public Filter getDocumentFilter() {
		return filterQuery;
	}

}
