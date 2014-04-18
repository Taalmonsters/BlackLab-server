package nl.inl.blacklab.server.search;

import java.util.Map;
import java.util.TreeMap;

import nl.inl.blacklab.server.dataobject.DataObject;
import nl.inl.blacklab.server.dataobject.DataObjectMapElement;

import org.apache.log4j.Logger;

/**
 * Uniquely describes a search operation.
 *
 * Used for caching and nonblocking operation.
 *
 * Derives from TreeMap because it keeps entries in sorted order, which can  be convenient.
 */
public class SearchParameters extends TreeMap<String, String> {
	private static final Logger logger = Logger.getLogger(SearchParameters.class);

	public SearchParameters() {
		// (nothing to do)
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (Map.Entry<String, String> e: entrySet()) {
			if (b.length() > 0)
				b.append(", ");
			b.append(e.getKey() + "=" + e.getValue());
		}
		return "{ " + b.toString() + " }";
	}

	public int getInteger(String name) {
		String value = get(name);
		try {
			return SearchUtil.strToInt(value);
		} catch (IllegalArgumentException e) {
			logger.debug("Illegal integer value for parameter '" + name + "': " + value);
			return 0;
		}
	}

	public boolean getBoolean(String name) {
		String value = get(name);
		try {
			return SearchUtil.strToBool(value);
		} catch (IllegalArgumentException e) {
			logger.debug("Illegal boolean value for parameter '" + name + "': " + value);
			return false;
		}
	}

	public SearchParameters copyWithJobClass(String newJobClass) {
		SearchParameters par = new SearchParameters();
		par.putAll(this);
		par.put("jobclass", newJobClass);
		return par;
	}

	public SearchParameters copyWithOnly(String... keys) {
		SearchParameters copy = new SearchParameters();
		for (String key: keys) {
			copy.put(key, get(key));
		}
		return copy;
	}

	public SearchParameters copyWithout(String... remove) {
		SearchParameters copy = new SearchParameters();
		copy.putAll(this);
		for (String key: remove) {
			copy.remove(key);
		}
		return copy;
	}

	public DataObject toDataObject() {
		DataObjectMapElement d = new DataObjectMapElement();
		for (Map.Entry<String, String> e: entrySet()) {
			d.put(e.getKey(), e.getValue());
		}
		return d;
	}

}
