package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.quarkus.security.UnauthorizedException;
import java.util.Base64;
import java.util.Objects;

/**
 * Interface for what it means to have credentials for the CQL driver
 *
 * <p>Create instances using the {@link CqlCredentialsFactory} class.
 *
 * <p><b>NOTE:</b> Implementations should be immutable, and support comparison and hashing because
 * they are used as part of the Session cache key. The initial ones use records for these reasons.
 */
public interface CqlCredentials {

  /**
   * Prefix for an auth token that is a username and password. See {@link
   * UsernamePasswordCredentials#fromToken(String)} for the format.
   */
  String USERNAME_PASSWORD_TOKEN_PREFIX = "Cassandra:";

  /** If the credentials are anonymous, i.e. there is no auth token or username/password. */
  default boolean isAnonymous() {
    return false;
  }

  /** Add the credentials to the provided CqlSessionBuilder so it can log in appropriately. */
  CqlSessionBuilder addToSessionBuilder(CqlSessionBuilder builder);

  /**
   * Credentials when the user has not provided an auth token.
   *
   * <p>--
   */
  record AnonymousCredentials() implements CqlCredentials {

    @Override
    public boolean isAnonymous() {
      return true;
    }

    @Override
    public CqlSessionBuilder addToSessionBuilder(CqlSessionBuilder builder) {
      // do nothing, there is no auth token
      return builder;
    }

    @Override
    public String toString() {
      return "AnonymousCredentials{isAnonymous=true}";
    }
  }

  /**
   * Credentials when the user has provided an auth token.
   *
   * @param token auth token passed, e.g. passed on the request, must be non-null and non-blank
   */
  record TokenCredentials(String token) implements CqlCredentials {

    public TokenCredentials {
      if (token == null || token.isBlank()) {
        throw new IllegalArgumentException("token must not be null or blank");
      }
    }

    @Override
    public CqlSessionBuilder addToSessionBuilder(CqlSessionBuilder builder) {
      return builder.withAuthCredentials("token", token);
    }

    @Override
    public String toString() {
      // Don't log the full token, just the first 4 chars
      return new StringBuilder("TokenCredentials{")
          .append("token='")
          .append(token.substring(0, 4))
          .append("...'")
          .append(", isAnonymous=")
          .append(isAnonymous())
          .append('}')
          .toString();
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
      // allow empty string, up to DB to validate
      Objects.requireNonNull(userName, "userName must not be null");
      Objects.requireNonNull(password, "password must not be null");
    }

    @Override
    public CqlSessionBuilder addToSessionBuilder(CqlSessionBuilder builder) {
      return builder.withAuthCredentials(userName, password);
    }

    @Override
    public String toString() {
      // Don't include any username or password
      return new StringBuilder("UsernamePasswordCredentials{")
          .append("userName='REDACTED'")
          .append(", password='REDACTED'")
          .append(", isAnonymous=")
          .append(isAnonymous())
          .append('}')
          .toString();
    }

    static UsernamePasswordCredentials fromToken(String encodedCredentials) {
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
