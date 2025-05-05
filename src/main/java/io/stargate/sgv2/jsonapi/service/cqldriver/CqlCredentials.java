package io.stargate.sgv2.jsonapi.service.cqldriver;

import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import java.util.Base64;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public CqlSessionBuilder addToSessionBuilder(CqlSessionBuilder builder) {
      return builder.withAuthCredentials(USERNAME_TOKEN, token);
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

  /** Factory to create the CqlCredentials based on the provided tokens. */
  class CqlCredentialsFactory implements CQLSessionCache.CredentialsFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CqlCredentialsFactory.class);

    private final DatabaseType databaseType;
    private final String fixedToken;
    private final String fixedUserName;
    private final String fixedPassword;

    /**
     * Constructor for the CqlCredentialsFactory.
     *
     * @param fixedToken the "fixed token" from configuration, e.g. from <code>
     *     operationsConfig.databaseConfig().fixedToken()</code> is passed in to make testing
     *     easier. When not null, this token is used to validate the authToken provided in the
     *     request.
     * @param fixedUserName the username to use if the fixedToken is set, this is from config
     *     usually
     * @param fixedPassword the password to use if the fixedToken is set, this is from config
     *     usually
     * @param databaseType the type of database, used to determine if anonymous access is allowed.
     */
    public CqlCredentialsFactory(
        String fixedToken, String fixedUserName, String fixedPassword, DatabaseType databaseType) {

      this.fixedToken = fixedToken;
      this.fixedUserName = fixedUserName;
      this.fixedPassword = fixedPassword;
      this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");

      if (fixedToken != null && LOGGER.isWarnEnabled()) {
        LOGGER.warn("Fixed token is set, all tokens will be validated against this token.");
      }
    }

    /** Create the CqlCredentials based on the provided authToken. */
    @Override
    public CqlCredentials apply(String authToken) {

      // This used to be in CqlSessionCache.getSession(), the fixedToken config is used in testing
      // and
      // the API
      // checks the provided authToken is the same as the configured fixedToken.
      if (fixedToken != null && !fixedToken.equals(authToken)) {
        throw new UnauthorizedException(ErrorCodeV1.UNAUTHENTICATED_REQUEST.getMessage());
      }

      // Also from CqlSessionCache.getNewSession(), if the fixedToken is set, then we always use the
      // configured / fallback username and password
      if (fixedToken != null) {
        return new UsernamePasswordCredentials(fixedUserName, fixedPassword);
      }

      var credentials =
          switch (authToken) {
            case null -> new AnonymousCredentials();
            case "" -> new AnonymousCredentials();
            case String t when t.startsWith(USERNAME_PASSWORD_TOKEN_PREFIX) ->
                UsernamePasswordCredentials.fromToken(t);
            default -> new TokenCredentials(authToken);
          };

      // Only the OFFLINE_WRITER allows anonymous access, because it is not connecting to an actual
      // database
      if (credentials.isAnonymous() && databaseType != DatabaseType.OFFLINE_WRITER) {
        throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
            "Missing/Invalid authentication credentials provided for type: %s", databaseType);
      }
      return credentials;
    }
  }
}
