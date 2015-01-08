package nl.inl.blacklab.server.requesthandlers;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.BlackLabServer;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;
import nl.inl.blacklab.server.search.User;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;

/**
 * Display the contents of the cache.
 */
public class RequestHandlerAddToIndex extends RequestHandler {
	
	private static final long MAX_UPLOAD_SIZE = 10000000;

	private static final int MAX_MEM_UPLOAD_SIZE = 1000000;
	
	private static final File TMP_DIR = new File(System.getProperty("java.io.tmpdir"));

	public RequestHandlerAddToIndex(BlackLabServer servlet,
			HttpServletRequest request, User user, String indexName,
			String urlResource, String urlPathPart) {
		super(servlet, request, user, indexName, urlResource, urlPathPart);
	}

	@Override
	public Response handle() {
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
			List<FileItem> fileItems = upload.parseRequest(request);

			// Process the uploaded file items
			Iterator<FileItem> i = fileItems.iterator();

			File filePath = servlet.getSearchManager().getIndexDir(indexName);
			int filesDone = 0;
			while (i.hasNext()) {
				FileItem fi = i.next();
				if (!fi.isFormField()) {
					
					if (filesDone != 0)
						return Response.internalError("Tried to upload more than one file.", debugMode, 14);
					
					// Get the uploaded file parameters
					String fieldName = fi.getFieldName();
					String fileName = fi.getName();
					String contentType = fi.getContentType();
					boolean isInMemory = fi.isInMemory();
					long sizeInBytes = fi.getSize();
					System.out.println("fieldName = " + fieldName);
					System.out.println("fileName = " + fileName);
					System.out.println("contentType = " + contentType);
					System.out.println("isInMemory = " + isInMemory);
					System.out.println("sizeInBytes = " + sizeInBytes);
					
					// Write the file
					fileName = new File(fileName).getName();  // strip path, if any
					File file = new File(filePath, fileName);
					fi.write(file);
					
					filesDone++;
				}
			}
		} catch (Exception ex) {
			System.out.println(ex);
		}

		DataObjectMapElement responseData = new DataObjectMapElement();
		responseData.put("result", "nothing");
		return new Response(responseData);
	}

}
