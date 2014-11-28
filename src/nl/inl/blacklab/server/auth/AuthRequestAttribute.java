package nl.inl.blacklab.server.auth;

import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;

import nl.inl.blacklab.server.search.User;

/**
 * Authentication system using servlet request attributes for logged-in user id.
 * 
 * Can be used, for example, with Shibboleth authentication.
 */
public class AuthRequestAttribute {
	
	private String attributeName;

	public AuthRequestAttribute(Map<String, Object> parameters) {
		this.attributeName = parameters.get("attributeName").toString();
	}
	
	public User determineCurrentUser(HttpServlet servlet,
			HttpServletRequest request) {
		
		// See if there's a logged-in user or not
		String userId = request.getAttribute(attributeName).toString();
		
		// Return the appropriate User object
		String sessionId = request.getSession().getId();
		if (userId == null || userId.length() == 0) {
			return User.anonymous(sessionId);
		}
		return User.loggedIn(userId, sessionId);
	}

}
