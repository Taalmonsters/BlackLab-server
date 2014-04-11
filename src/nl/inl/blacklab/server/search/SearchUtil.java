package nl.inl.blacklab.server.search;

public class SearchUtil {

	public static int strToInt(String value) throws IllegalArgumentException {
		try {
			return Integer.parseInt(value);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Cannot convert to int: " + value);
		}
	}

	public static boolean strToBool(String value) throws IllegalArgumentException {
		if (value.equals("true") || value.equals("1") || value.equals("yes") || value.equals("on"))
			return true;
		if (value.equals("false") || value.equals("0") || value.equals("no") || value.equals("off"))
			return false;
		throw new IllegalArgumentException("Cannot convert to boolean: " + value);
	}

}
