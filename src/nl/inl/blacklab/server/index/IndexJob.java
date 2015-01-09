package nl.inl.blacklab.server.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import nl.inl.blacklab.index.DocIndexer;
import nl.inl.blacklab.index.Indexer;

import org.apache.log4j.Logger;

public class IndexJob {

	private static final Logger logger = Logger.getLogger(IndexJob.class);

	/** The data we're indexing. We're responsible for closing the stream when
	 *  we're done with it. */
	private InputStream data;
	
	private String name;

	private File indexDir;

	private Class<? extends DocIndexer> docIndexerClass;

	/**
	 * Construct a new SearchThread
	 * @param indexDir directory of index to add to
	 * @param docIndexerClass DocIndexer class to use (=input type)
	 * @param data (XML) input data
	 * @param name (file) name for the input data
	 * @param search the search to execute in the thread
	 */
	public IndexJob(File indexDir, Class<? extends DocIndexer> docIndexerClass, InputStream data, String name) {
		this.indexDir = indexDir;
		this.docIndexerClass = docIndexerClass;
		this.data = data;
		this.name = name;
		logger.debug("IndexJob constructor");
	}

	public void run() throws Exception {
		Indexer indexer = null;
		try {
			indexer = new Indexer(indexDir, false, docIndexerClass);
			try {
				Reader reader = new BufferedReader(new InputStreamReader(data, "utf-8"));
				try {
					logger.debug("Starting indexing");
					indexer.index(name, reader);
					logger.debug("Done indexing");
				} finally {
					reader.close();
				}
			} finally {
				indexer.close();
			}
		} finally {
			data.close();
			data = null;
		}
	}
}
