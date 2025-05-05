package io.stargate.sgv2.jsonapi.config;

import io.stargate.sgv2.jsonapi.service.cqldriver.CqlCredentials;
import java.util.Objects;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * The back end database the API is running against.
 *
 * <p>How we manage credentials is a bit different for each database type, see {@link
 * CqlCredentials.CqlCredentialsFactory}.
 */
public enum DatabaseType {
  ASTRA,
  CASSANDRA,
  OFFLINE_WRITER;

  /**
   * Constants should only be used where we need a string constant for defaults etc, use the enum
   * normally.
   */
  public interface Constants {
    String ASTRA = "ASTRA";
    String CASSANDRA = "CASSANDRA";
    String OFFLINE_WRITER = "OFFLINE_WRITER";
  }

  /** Used by {@link OperationsConfig.DatabaseConfig#type} */
  public static class DatabaseTypeConverter implements Converter<DatabaseType> {
    @Override
    public DatabaseType convert(String value)
        throws IllegalArgumentException, NullPointerException {
      Objects.requireNonNull(value, "value must not be null");
      return DatabaseType.valueOf(value.toUpperCase());
    }
  }
}
