package nl.inl.blacklab.server.requesthandlers;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.search.Searcher;
import nl.inl.blacklab.search.indexstructure.AltDesc;
import nl.inl.blacklab.search.indexstructure.ComplexFieldDesc;
import nl.inl.blacklab.search.indexstructure.IndexStructure;
import nl.inl.blacklab.search.indexstructure.MetadataFieldDesc;
import nl.inl.blacklab.search.indexstructure.PropertyDesc;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapAttribute;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.IndexOpenException;
import nl.inl.util.StringUtil;

import org.apache.log4j.Logger;

/**
 * Get information about the structure of an index.
 */
public class RequestHandlerIndexStructure extends RequestHandler {
	@SuppressWarnings("hiding")
	private static final Logger logger = Logger.getLogger(RequestHandlerIndexStructure.class);

	public RequestHandlerIndexStructure(BlackLabServer servlet, HttpServletRequest request, String indexName, String urlResource, String urlPathPart) {
		super(servlet, request, indexName, urlResource, urlPathPart);
	}

	@Override
	public DataObject handle() throws IndexOpenException {
		debug(logger, "REQ struct: " + indexName);

		Searcher searcher = searchMan.getSearcher(indexName);
		IndexStructure struct = searcher.getIndexStructure();

		// Complex fields
		DataObjectMapAttribute doComplexFields = new DataObjectMapAttribute("complexField", "name");
		for (String name: struct.getComplexFields()) {
			ComplexFieldDesc fieldDesc = struct.getComplexFieldDesc(name);
			DataObjectMapElement doComplexField = new DataObjectMapElement();
			doComplexField.put("displayName", fieldDesc.getDisplayName());
			doComplexField.put("description", fieldDesc.getDescription());
			doComplexField.put("hasContentStore", fieldDesc.hasContentStore());
			doComplexField.put("hasXmlTags", fieldDesc.hasXmlTags());
			doComplexField.put("hasLengthTokens", fieldDesc.hasLengthTokens());
			doComplexField.put("mainProperty", fieldDesc.getMainProperty().getName());
			DataObjectMapAttribute doProps = new DataObjectMapAttribute("property", "name");
			for (String propName: fieldDesc.getProperties()) {
				PropertyDesc propDesc = fieldDesc.getPropertyDesc(propName);
				DataObjectMapElement doProp = new DataObjectMapElement();
				doProp.put("hasForwardIndex", propDesc.hasForwardIndex());
				DataObjectMapAttribute doAlts = new DataObjectMapAttribute("alternative", "name");
				for (String altName: propDesc.getAlternatives()) {
					AltDesc altDesc = propDesc.getAlternativeDesc(altName);
					DataObjectMapElement doAlt = new DataObjectMapElement();
					doAlt.put("type", altDesc.getType().toString());
					doAlt.put("hasOffsets", altDesc == propDesc.getOffsetsAlternative());
					doAlts.put(altName, doAlt);
				}
				doProp.put("alternative", doAlts);
				doProps.put(propName, doProp);
			}
			doComplexField.put("properties", doProps);
			doComplexFields.put(name, doComplexField);
		}

		// Metadata fields
		DataObjectMapAttribute doMetaFields = new DataObjectMapAttribute("metadataField", "name");
		for (String name: struct.getMetadataFields()) {
			MetadataFieldDesc fd = struct.getMetadataFieldDesc(name);
			DataObjectMapElement doMetaField = new DataObjectMapElement();
			doMetaField.put("fieldName", fd.getName());
			doMetaField.put("displayName", fd.getDisplayName());
			doMetaField.put("type", fd.getType().toString());
			doMetaFields.put(name, doMetaField);
		}
		
		DataObjectMapElement doVersionInfo = new DataObjectMapElement();
		doVersionInfo.put("blackLabBuildDate", struct.getBlackLabBuildDate());
		doVersionInfo.put("indexFormat", struct.getIndexFormat());
		doVersionInfo.put("timeCreated", struct.getTimeCreated());
		doVersionInfo.put("timeModified", struct.getTimeModified());

		DataObjectMapElement doFieldInfo = new DataObjectMapElement();
		doFieldInfo.put("pidField", StringUtil.nullToEmpty(struct.pidField()));
		doFieldInfo.put("titleField", StringUtil.nullToEmpty(struct.titleField()));
		doFieldInfo.put("authorField", StringUtil.nullToEmpty(struct.authorField()));
		doFieldInfo.put("dateField", StringUtil.nullToEmpty(struct.dateField()));
		doFieldInfo.put("complexFields", doComplexFields);
		doFieldInfo.put("metadataFields", doMetaFields);
		
		// Assemble response
		DataObjectMapElement response = new DataObjectMapElement();
		response.put("indexName", indexName);
		response.put("displayName", struct.getDisplayName());
		response.put("description", struct.getDescription());
		response.put("versionInfo", doVersionInfo);
		response.put("fieldInfo", doFieldInfo);
		
		// Remove any empty settings
		response.removeEmptyMapValues();

		return response;
	}

}
