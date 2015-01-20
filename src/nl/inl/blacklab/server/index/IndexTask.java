package nl.inl.blacklab.server.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import nl.inl.blacklab.index.DocIndexer;
import nl.inl.blacklab.index.IndexListener;
import nl.inl.blacklab.index.IndexListenerDecorator;
import nl.inl.blacklab.index.Indexer;
import nl.inl.blacklab.indexers.DocIndexerTei;

import org.apache.log4j.Logger;

public class IndexTask {

	private static final Logger logger = Logger.getLogger(IndexTask.class);

	/** The data we're indexing. We're responsible for closing the stream when
	 *  we're done with it. */
	private InputStream data;
	
	private String name;

	private File indexDir;

	private Class<? extends DocIndexer> docIndexerClass;

	private File dataFile;

	private IndexListener decoratedListener;
	
	String indexError = null;

	/**
	 * Construct a new SearchThread
	 * @param indexDir directory of index to add to
	 * @param docIndexerClass DocIndexer class to use (=input type)
	 * @param data (XML) input data
	 * @param name (file) name for the input data
	 * @param listener the index listener to use
	 */
	public IndexTask(File indexDir, Class<? extends DocIndexer> docIndexerClass, 
			InputStream data, String name, IndexListener listener) {
		this.indexDir = indexDir;
		this.docIndexerClass = docIndexerClass;
		this.data = data;
		this.name = name;
		setListener(listener);
	}

	public IndexTask(File indexDir, Class<DocIndexerTei> docIndexerClass,
			File dataFile, String name, IndexListener listener) {
		this.indexDir = indexDir;
		this.docIndexerClass = docIndexerClass;
		this.dataFile = dataFile;
		this.name = name;
		setListener(listener);
	}

	private void setListener(IndexListener listener) {
		this.decoratedListener = new IndexListenerDecorator(listener) {
			@Override
			public boolean errorOccurred(String error, String unitType,
					File unit, File subunit) {
				indexError = error;
				return super.errorOccurred(error, unitType, unit, subunit);
			}
		};
	}

	public void run() throws Exception {
		Indexer indexer = null;
		try {
			indexer = new Indexer(indexDir, false, docIndexerClass);
			indexer.setListener(decoratedListener);
			indexer.setContinueAfterInputError(false);
			indexer.setRethrowInputError(false);
			try {
				if (data == null && dataFile != null) {
					// Used for zip files, possibly other types in the future.
					indexer.index(dataFile);
				} else if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
					// Tar gzipped data; read directly from stream.
					indexer.indexTarGzip(name, data, "*.xml", true);
				} else {
					// Straight XML data. Read as UTF-8.
					Reader reader = new BufferedReader(new InputStreamReader(data, "utf-8"));
					try {
						logger.debug("Starting indexing");
						indexer.index(name, reader);
						logger.debug("Done indexing");
					} finally {
						reader.close();
					}
				}
			} catch (Exception e) {
				logger.warn("An error occurred while indexing, rolling back changes: " + e.getMessage());
				indexer.rollback();
				indexer = null;
				throw e;
			} finally {
				if (indexError != null) {
					logger.warn("An error occurred while indexing, rolling back changes: " + indexError);
					if (indexer != null)
						indexer.rollback();
					indexer = null;
					indexError = null;
				} else {
					if (indexer != null)
						indexer.close();
					indexer = null;
				}
			}
		} finally {
			if (data != null)
				data.close();
			data = null;
		}
	}
}
