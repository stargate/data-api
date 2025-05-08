package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.Base64;

/**
 * Interface for what it means to have credentials for the CQL driver
 *
 * <p>Create instances using the {@link #create(String, String, String, String)} factory method,
 * this will return the correct implementation based on the provided tokens.
 *
 * <p><b>NOTE:</b> Implementations should be immutable, and support comparison and hashing because
 * they are used as part of the Session cache key. The initial ones use records for these reasons.
 */
public interface CqlCredentials {

  String USERNAME_PASSWORD_PREFIX = "Cassandra:";

  /**
   * Factory method to create the correct CqlCredentials based on the provided tokens.
   *
   * @param fixedToken the "fixed token" from configuration, e.g. from <code>
   *     operationsConfig.databaseConfig().fixedToken()</code> is passed in to make testing easier.
   * @param authToken the token provided in the request, e.g. from the Authorization / Token header
   * @param fallbackUsername the username to use if the fixedToken is set, this is from config
   *     usually
   * @param fallbackPassword the password to use if the fixedToken is set, this is from config
   *     usually
   * @return the correct CqlCredentials implementation based on the provided tokens
   */
  static CqlCredentials create(
      String fixedToken, String authToken, String fallbackUsername, String fallbackPassword) {

    // This used to be in CqlSessionCache.getSession(), the fixedToken config is used in testing and
    // the API
    // checks the provided authToken is the same as the configured fixedToken.
    if (fixedToken != null && !fixedToken.equals(authToken)) {
      throw new UnauthorizedException(ErrorCodeV1.UNAUTHENTICATED_REQUEST.getMessage());
    }

    // Also from CqlSessionCache.getNewSession(), if the fixedToken is set, then we always use the
    // configured / fallback username and password
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

  /** If the credentials are anonymous, i.e. there is no auth token or username/password. */
  default boolean isAnonymous() {
    return false;
  }

  /** Add the credentials to the provided CqlSessionBuilder so it can login appropriately. */
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
   * Credentials when the user has provided an auth token.
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
      } catch (IllegalArgumentException e) {
        throw new UnauthorizedException(
            "Invalid credentials format, expected `Cassandra:Base64(username):Base64(password)`");
      }
    }
  }
}
