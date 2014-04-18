package nl.inl.blacklab.server.search;

import java.io.File;

import org.json.JSONObject;

public class JsonUtil {

	public static String getProperty(JSONObject obj, String key, String defVal) {
		if (!obj.has(key))
			return defVal;
		return obj.getString(key);
	}

	public static File getFileProp(JSONObject obj, String key, File defVal) {
		if (!obj.has(key))
			return defVal;
		return new File(obj.getString(key));
	}

	public static boolean getBooleanProp(JSONObject obj, String key, boolean defVal) {
		if (!obj.has(key))
			return defVal;
		return obj.getBoolean(key);
	}

	public static int getIntProp(JSONObject obj, String key, int defVal) {
		if (!obj.has(key))
			return defVal;
		return obj.getInt(key);
	}

	public static long getLongProp(JSONObject obj, String key, long defVal) {
		if (!obj.has(key))
			return defVal;
		return obj.getLong(key);
	}

}
