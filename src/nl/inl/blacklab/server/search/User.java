package nl.inl.blacklab.server.search;

/** Represents either a unique (logged-in) user, or a unique session 
 *  (when not logged in). */
public class User {
	/** The user id if logged in; null otherwise */
	private String userId;
	
	/** The session id */
	private String sessionId;
	
	public User(String userId, String sessionId) {
		this.userId = userId;
		if (userId != null && userId.length() == 0)
			this.userId = null;
		this.sessionId = sessionId;
	}

	@Override
	public String toString() {
		return userId != null ? userId : "SESSION:" + sessionId;
	}
	
	public String uniqueId() {
		return userId != null ? userId : "S:" + sessionId;
	}

	public String uniqueIdShort() {
		String str = uniqueId();
		return str.length() > 6 ? str.substring(0, 6) : str;
	}
	
	public boolean isLoggedIn() {
		return userId != null;
	}
	
	public String getUserId() {
		return userId;
	}

	public String getSessionId() {
		return sessionId;
	}

	
}
