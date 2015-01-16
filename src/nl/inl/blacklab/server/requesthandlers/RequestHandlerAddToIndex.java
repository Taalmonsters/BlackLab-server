package nl.inl.blacklab.server.requesthandlers;

import java.io.File;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.indexers.DocIndexerTei;
import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.exceptions.BlsException;
import nl.inl.blacklab.server.exceptions.IndexNotFound;
import nl.inl.blacklab.server.exceptions.NotAuthorized;
import nl.inl.blacklab.server.index.IndexTask;
import nl.inl.blacklab.server.search.User;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerAddToIndex extends RequestHandler {
	
	private static final long MAX_UPLOAD_SIZE = 20000000;

	private static final int MAX_MEM_UPLOAD_SIZE = 1000000;
	
	private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

	public RequestHandlerAddToIndex(BlackLabServer servlet,
			HttpServletRequest request, User user, String indexName,
			String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public Response handle() throws BlsException {
		if (!indexName.contains(":"))
			throw new NotAuthorized("Can only add to private indices.");
		if (!searchMan.indexExists(indexName))
			throw new IndexNotFound(indexName);
		
		String status = searchMan.getIndexStatus(indexName);
		if (!status.equals("available"))
			return Response.unavailable(indexName, status);

		// Check that we have a file upload request
		boolean isMultipart = ServletFileUpload.isMultipartContent(request);
		if (!isMultipart) {
			return Response.badRequest("NO_FILE", "Upload a file to add to the index.");
		}
		DiskFileItemFactory factory = new DiskFileItemFactory();
		
		// maximum size that will be stored in memory
		factory.setSizeThreshold(MAX_MEM_UPLOAD_SIZE);
		// Location to save data that is larger than maxMemSize.
		factory.setRepository(TMP_DIR);

		// Create a new file upload handler
		ServletFileUpload upload = new ServletFileUpload(factory);
		// maximum file size to be uploaded.
		upload.setSizeMax(MAX_UPLOAD_SIZE);

		try {
			// Parse the request to get file items.
			List<FileItem> fileItems;
			try {
				fileItems = upload.parseRequest(request);
			} catch (FileUploadException e) {
				return Response.badRequest("ERROR_UPLOADING_FILE", e.getMessage());
			}

			// Process the uploaded file items
			Iterator<FileItem> i = fileItems.iterator();

			if (!searchMan.indexExists(indexName))
				return Response.indexNotFound(indexName);
			File indexDir = searchMan.getIndexDir(indexName);
			int filesDone = 0;
			while (i.hasNext()) {
				FileItem fi = i.next();
				if (!fi.isFormField()) {
					
					if (!fi.getFieldName().equals("data"))
						return Response.badRequest("CANNOT_UPLOAD_FILE", "Cannot upload file. File should be uploaded using the 'data' field.");
					
					if (fi.getSize() > MAX_UPLOAD_SIZE)
						return Response.badRequest("CANNOT_UPLOAD_FILE", "Cannot upload file. Too large.");
					
					if (filesDone != 0)
						return Response.internalError("Tried to upload more than one file.", debugMode, 14);
					
					// Get the uploaded file parameters
					String fileName = fi.getName();
					//String contentType = fi.getContentType();
					//boolean isInMemory = fi.isInMemory();
					/*System.out.println("fileName = " + fileName);
					System.out.println("contentType = " + contentType);
					System.out.println("isInMemory = " + isInMemory);*/
					
					InputStream data = fi.getInputStream();

					// TODO: do this in the background
					// TODO: lock the index while indexing
					// TODO: re-open Searcher after indexing
					// TODO: keep track of progress
					// TODO: error handling
					IndexTask task = new IndexTask(indexDir, DocIndexerTei.class, data, fileName);
					task.run();
					
					//searchMan.addIndexTask(indexName, new IndexTask(is, fileName));
					
					/*
					// Write the file
					fileName = new File(fileName).getName();  // strip path, if any
					File file = new File(indexDir, fileName);
					fi.write(file);
					*/
					
					filesDone++;
				}
			}
		} catch (Exception ex) {
			return Response.internalError(ex, debugMode, 26);
		}

//		DataObjectMapElement responseData = new DataObjectMapElement();
//		responseData.put("result", "nothing");
//		Response r = new Response(responseData);
//		r.setCacheAllowed(false);
//		return r;
		return Response.accepted();
	}
}
