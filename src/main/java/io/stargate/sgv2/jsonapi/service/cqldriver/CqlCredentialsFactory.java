package io.stargate.sgv2.jsonapi.service.cqldriver;

import io.quarkus.security.UnauthorizedException;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.ServerException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory to create the CqlCredentials based on the provided tokens. */
public class CqlCredentialsFactory implements CQLSessionCache.CredentialsFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(CqlCredentialsFactory.class);

  private final DatabaseType databaseType;
  private final String fixedToken;
  private final String fixedUserName;
  private final String fixedPassword;

  /**
   * Constructor for the CqlCredentialsFactory.
   *
   * @param databaseType the type of database, used to determine if anonymous access is allowed.
   * @param fixedToken the "fixed token" from configuration, e.g. from <code>
   *                      operationsConfig.databaseConfig().fixedToken()</code> is passed in to make
   *     testing easier. When not null, this token is used to validate the authToken provided in the
   *     request.
   * @param fixedUserName the username to use if the fixedToken is set, this is from config usually
   * @param fixedPassword the password to use if the fixedToken is set, this is from config usually
   */
  public CqlCredentialsFactory(
      DatabaseType databaseType, String fixedToken, String fixedUserName, String fixedPassword) {

    this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.fixedToken = fixedToken;
    this.fixedUserName = fixedUserName;
    this.fixedPassword = fixedPassword;

    if (fixedToken != null) {
      LOGGER.warn("Fixed token is set, all tokens will be validated against this token.");
    }
  }

  /** Create the CqlCredentials based on the provided authToken. */
  @Override
  public CqlCredentials apply(String authToken) {

    if (fixedToken != null) {
      // The fixedToken config is used for testing and for the API to verify that
      // the provided authToken matches the configured fixedToken.
      // (Previously part of CqlSessionCache.getSession())
      if (!fixedToken.equals(authToken)) {
        throw new UnauthorizedException(ErrorCodeV1.UNAUTHENTICATED_REQUEST.getMessage());
      }
      // If a fixedToken is configured, always use the fallback username and password.
      // (Logic originally from CqlSessionCache.getNewSession())
      return new CqlCredentials.UsernamePasswordCredentials(fixedUserName, fixedPassword);
    }

    var credentials =
        switch (authToken) {
          case null -> new CqlCredentials.AnonymousCredentials();
          case "" -> new CqlCredentials.AnonymousCredentials();
          case String t when t.startsWith(CqlCredentials.USERNAME_PASSWORD_TOKEN_PREFIX) ->
              CqlCredentials.UsernamePasswordCredentials.fromToken(t);
          default -> new CqlCredentials.TokenCredentials(authToken);
        };

    // Only the OFFLINE_WRITER allows anonymous access, because it is not connecting to an actual
    // database
    if (credentials.isAnonymous() && databaseType != DatabaseType.OFFLINE_WRITER) {
      throw ServerException.internalServerError(
          "Missing/Invalid authentication credentials provided for type: " + databaseType);
    }
    return credentials;
  }
}
