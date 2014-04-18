package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

import org.apache.log4j.Logger;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerCacheInfo extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerCacheInfo.class);

	public RequestHandlerCacheInfo(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() {
		debug(logger, "REQ cacheinfo");

		DataObjectMapElement response = new DataObjectMapElement();
		response.put("cache-status", searchMan.getCacheStatusDataObject());
		response.put("cache-contents", searchMan.getCacheContentsDataObject());

		return response;
	}

}
