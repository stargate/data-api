package io.stargate.sgv2.jsonapi.config;

import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import io.stargate.sgv2.jsonapi.service.cqldriver.CqlCredentialsFactory;
import java.util.Objects;
import org.eclipse.microprofile.config.spi.Converter;

/**
 * The back end database the API is running against.
 *
 * <p>How we manage credentials is a bit different for each database type, see {@link
 * CqlCredentialsFactory}.
 */
public enum DatabaseType {
  ASTRA(false),
  CASSANDRA(true),
  OFFLINE_WRITER(false);

  private final boolean singleTenant;

  DatabaseType(boolean singleTenant) {
    this.singleTenant = singleTenant;
  }

  public boolean isSingleTenant() {
    return singleTenant;
  }

  public String normalizeValidateTenantId(String tenantId) {

    var normalized = normalizeOptionalString(tenantId).toUpperCase();
    if (isSingleTenant()) {
      if (!normalized.isEmpty()) {
        throw new IllegalArgumentException(
            "DatabaseType %s is singleTenant, tenantId must be empty, but was: '%s'"
                .formatted(this.name(), normalized));
      }
      return normalized;
    }

    // For multi-tenant databases, must not be
    if (normalized.isEmpty()) {
      throw new IllegalArgumentException(
          "DatabaseType %s is multi-tenant, tenantId must not be empty".formatted(this.name()));
    }
    return normalized;
  }

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
