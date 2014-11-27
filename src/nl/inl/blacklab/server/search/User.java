package nl.inl.blacklab.server.search;

import nl.inl.util.FileUtil;

/** Represents either a unique (logged-in) user, or a unique session 
 *  (when not logged in). */
public class User {
	/** The user id if logged in; null otherwise */
	private String userId;
	
	/** The session id */
	private String sessionId;
	
	/**
	 * Create a new logged-in user.
	 * 
	 * @param userId unique id identifying this user
	 * @param sessionId the session id
	 * @return the new user
	 */
	public static User loggedIn(String userId, String sessionId) {
		return new User(userId, sessionId);
	}
	
	/**
	 * Create a new anonymous user.
	 * 
	 * @param sessionId the session id
	 * @return the new user
	 */
	public static User anonymous(String sessionId) {
		return new User(null, sessionId);
	}
	
	private User(String userId, String sessionId) {
		this.userId = null;
		if (userId != null) {
			// Strip any colons from userid because we use colon as a separator
			// between userid and index name.
			this.userId = userId.replaceAll(":", "");
			if (this.userId.length() == 0)
				this.userId = null;
		}
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

	public String getUserDirName() {
		return FileUtil.sanitizeFilename(userId);
	}

	public String getSessionId() {
		return sessionId;
	}

	
}
