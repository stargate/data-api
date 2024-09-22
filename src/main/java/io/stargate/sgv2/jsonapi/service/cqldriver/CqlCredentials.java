package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.Base64;

/** A marker interface for credentials. */
interface CqlCredentials {

  String USERNAME_PASSWORD_PREFIX = "Cassandra:";

  /**
   * Factory method to return the correct
   *
   * @param token
   * @return
   */
  static CqlCredentials create(
      String fixedToken, String authToken, String fallbackUsername, String fallbackPassword) {

    // This used to be in CqlSessionCache.getSession(), the fixedToken config is used in testing and
    // the API
    // checks the provided authToken is the same as the configured fixedToken.
    // TODO: refactor / rename the "fixedToken" why is the API checking the tokens ?
    if (fixedToken != null && !fixedToken.equals(authToken)) {
      throw new UnauthorizedException(ErrorCodeV1.UNAUTHENTICATED_REQUEST.getMessage());
    }

    // Also from CqlSessionCache.getNewSession(), if the fixedToken is set, then we always use the
    // configured
    // username and password
    // TODO: confirm the logic here, we have a fixedToken that is a string we ignore the value of
    if (fixedToken != null) {
      return new UsernamePasswordCredentials(fallbackUsername, fallbackPassword);
    }

    return switch (authToken) {
      case null -> new AnonymousCredentials();
      case "" -> new AnonymousCredentials();
      case String t when t.startsWith(USERNAME_PASSWORD_PREFIX) ->
          UsernamePasswordCredentials.fromToken(t);
      default -> new TokenCredentials(authToken);
    };
  }

  default boolean isAnonymous() {
    return false;
  }

  void addToSessionBuilder(CqlSessionBuilder builder);

  record AnonymousCredentials() implements CqlCredentials {

    @Override
    public boolean isAnonymous() {
      return true;
    }

    @Override
    public void addToSessionBuilder(CqlSessionBuilder builder) {
      // Do nothing
    }
  }

  /**
   * CqlCredentials for CQLSession cache when token is provided.
   *
   * @param token auth token passed, e.g. passed on the request, must be non-null and non-blank
   */
  record TokenCredentials(String token) implements CqlCredentials {

    /** CQL username to be used when using the auth token as the credentials */
    private static final String USERNAME_TOKEN = "token";

    public TokenCredentials {
      if (token == null || token.isBlank()) {
        throw new IllegalArgumentException("token must not be null or blank");
      }
    }

    @Override
    public void addToSessionBuilder(CqlSessionBuilder builder) {
      builder.withAuthCredentials(USERNAME_TOKEN, token);
    }
  }

  /**
   * CqlCredentials for CQLSession cache when username and password is provided.
   *
   * <p>Recommend creating using the factory method {@link #fromToken(String)}, the public
   * constructor is left because it may be useful.
   *
   * @param userName
   * @param password
   */
  record UsernamePasswordCredentials(String userName, String password) implements CqlCredentials {

    public UsernamePasswordCredentials {
      if (userName == null || userName.isBlank()) {
        throw new IllegalArgumentException("userName must not be null or blank");
      }
      if (password == null || password.isBlank()) {
        throw new IllegalArgumentException("password must not be null or blank");
      }
    }

    @Override
    public void addToSessionBuilder(CqlSessionBuilder builder) {
      builder.withAuthCredentials(userName, password);
    }

    public static UsernamePasswordCredentials fromToken(String encodedCredentials) {
      String[] parts = encodedCredentials.split(":");
      if (parts.length != 3) {
        throw new UnauthorizedException(
            "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
      }

      try {
        String userName = new String(Base64.getDecoder().decode(parts[1]));
        String password = new String(Base64.getDecoder().decode(parts[2]));
        return new UsernamePasswordCredentials(userName, password);
      } catch (Exception e) {
        // TODO: WHat exceptions are we expecting here ? Catch at Exception is bad practice
        throw new UnauthorizedException(
            "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
      }
    }
  }
}
