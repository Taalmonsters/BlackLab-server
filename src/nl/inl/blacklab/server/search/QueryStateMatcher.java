package nl.inl.blacklab.server.search;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import nl.inl.blacklab.server.exceptions.ConfigurationException;

public class QueryStateMatcher {
	
	List<String> conditionType = new ArrayList<String>();
	
	List<Integer> conditionValue = new ArrayList<Integer>();
	
	static List<String> validTypes = Arrays.asList("always", "never", "age", "ignored", "paused", "cached");

	/**
	 * Construct a query state matcher.
	 * 
     * The values for the matcher condition are made up of simple clauses, shown below.
     * Clauses may be combined using "or".
     * 
     * always:    matches all searches
     * never:     matches no searches
     * age n:     matches searches that were started at least n seconds ago
     * ignored n: matches searches that haven't been touched for at least n seconds
     *            (e.g. no-one has asked about the status of this search)
     * paused n:  matches searches that have been paused for at least n seconds
     * cached n:  matches finished searches for which the results have been cached 
     *            for at least n seconds.
	 * 
	 * @param condition
	 * @throws ConfigurationException
	 */
	public QueryStateMatcher(String condition) throws ConfigurationException {
		if (condition.contains(" and ") || condition.contains("not ") || condition.contains("!") || condition.contains("(")) {
			throw new ConfigurationException("Unsupported constructs used in query state expression (only 'or' operator is supported): " + condition);
		}
		String[] parts = condition.trim().split("\\s+or\\s+");
		for (String part: parts) {
			String[] words = part.split("\\s+");
			if (words.length > 2)
				throw new ConfigurationException("Too many values in query state expression: " + part);
			if (!validTypes.contains(words[0]))
				throw new ConfigurationException("Unknown query state expression type: " + part);
			conditionType.add(words[0]);
			if (words.length > 1) {
				try {
					conditionValue.add(Integer.parseInt(words[1]));
				} catch (NumberFormatException e) {
					throw new ConfigurationException("Illegal number in query state expression: " + part);
				}
			} else {
				conditionValue.add(0);
			}
		}
	}
	
	/**
	 * Check if any of the specified conditions matches
	 * @param query the query to match
	 * @return true if it matches the (one or more of the) condition(s)
	 */
	public boolean matches(Job query) {
		return whichClauseMatches(query) >= 0;
	}
	
	private int whichClauseMatches(Job query) {
		int matchingClause = -1;
		for (int i = 0; i < conditionType.size() && matchingClause == -1; i++) {
			String type = conditionType.get(i);
			Integer value = conditionValue.get(i);
			if (value == null)
				throw new RuntimeException("value == null");
			switch(type) {
			case "always":
				matchingClause = i;
				break;
			case "never":
				break;
			case "age":
				if (query.executionTime() >= value)
					matchingClause = i;
				break;
			case "ignored":
				if (query.notAccessedFor() >= value)
					matchingClause = i;
				break;
			case "paused":
				if (query.pausedFor() >= value)
					matchingClause = i;
				break;
			case "cached":
				if (query.cacheAge() >= value)
					matchingClause = i;
				break;
			default:
				throw new RuntimeException(); // Cannot happen, we checked..
			}
		}
		return matchingClause;
	}

	public String explainMatch(Job query) {
		int i = whichClauseMatches(query);
		String type = conditionType.get(i);
		switch(type) {
		case "always":
			return "all queries match";
		case "age":
			return "age is " + query.executionTime();
		case "ignored":
			return "ignored for " + query.notAccessedFor();
		case "paused":
			return "paused for " + query.notAccessedFor();
		case "cached":
			return "cached for " + query.cacheAge();
		default:
			throw new RuntimeException(); // Cannot happen, we checked..
		}
	}

	@Override
	public String toString() {
		StringBuilder b = new StringBuilder();
		for (int i = 0; i < conditionType.size(); i++) {
			if (b.length() > 0)
				b.append(" or ");
			String type = conditionType.get(i);
			Integer value = conditionValue.get(i);
			switch(type) {
			case "always":
			case "never":
				b.append(type);
				break;
			case "age":
			case "ignored":
			case "paused":
			case "cached":
				b.append(type + " " + value);
				break;
			default:
				throw new RuntimeException(); // Cannot happen, we checked..
			}
		}
		return b.toString();
	}
	
	
}
