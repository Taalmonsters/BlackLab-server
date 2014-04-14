package nl.inl.blacklab.server.search;

import java.util.HashMap;
import java.util.Map;

import nl.inl.blacklab.server.BlackLabServer;

import org.apache.log4j.Logger;

/**
 * Uniquely describes a search operation.
 *
 * Used for caching and nonblocking operation.
 */
public class SearchParameters extends HashMap<String, String> {
	private static final Logger logger = Logger.getLogger(BlackLabServer.class);

	public SearchParameters() {

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
		SearchParameters par = new SearchParameters();
		for (String key: keys) {
			par.put(key, get(key));
		}
		return par;
	}

}
