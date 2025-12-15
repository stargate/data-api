package io.stargate.sgv2.jsonapi.api.request.tenant;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.DatabaseType;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;

/**
 * The Tenant in the API, we always have a tenant even if the database is single-tenant.
 *
 * <p>Tenant is bound to the {@link DatabaseType} because that tells if the DB is a single tenant or
 * multi-tenant database.
 *
 * <p>Create instances using the {@link TenantFactory} because it will be configured with the
 * correct DatabaseType. Use the {@link Tenant#create(DatabaseType, String)} method to create for
 * tests where you want to set the DB type.
 *
 * <p>A tenant is identifier by non-null String tenant id, it is normalised and validated with
 * {@link DatabaseType#normalizeValidateTenantId(String)}. Which will make it UPPER CASE and is
 * compared as such. We assumed the tenantID is something like a UUID, that should compare as
 * case-insensitive, see {@link #equals(Object)}.
 *
 * <p><b>IMPORTANT:</b> Work with Tenant instances, rather than get the string tenant id.
 *
 * <p>Safe for multi threading.
 */
public class Tenant implements Recordable {

  // TenantID used when the DB is single-tenant.
  private static final String SINGLE_TENANT_ID = "SINGLE-TENANT";

  private static final Tenant SINGLE_TENANT_CASSANDRA =
      new Tenant(DatabaseType.CASSANDRA, SINGLE_TENANT_ID);

  private final DatabaseType databaseType;
  private final String tenantId;

  private Tenant(DatabaseType databaseType, String tenantId) {
    this.databaseType = databaseType;
    // this is just for sanity checking, the DB Type should have uppercased it
    this.tenantId = tenantId.toUpperCase();
  }

  /**
   * Factory method to create a Tenant instance.
   *
   * <p>Visible for testing, you should use the {@link TenantFactory} to create instances
   *
   * @param databaseType the type of database this tenant is for, must not be null.
   * @param tenantId the tenant id, must be null or blank for single-tenant. Otherwise, null is
   *     normalized, and validated to be non-blank.
   * @return a Tenant instance
   */
  @VisibleForTesting
  public static Tenant create(DatabaseType databaseType, String tenantId) {
    Objects.requireNonNull(databaseType, "databaseType must not be null");

    var normalizedValidated = databaseType.normalizeValidateTenantId(tenantId);

    if (databaseType.isSingleTenant()) {
      // use switch to reduce static instance based on database type
      return switch (databaseType) {
        case CASSANDRA -> SINGLE_TENANT_CASSANDRA;
        default ->
            throw new IllegalStateException(
                "Unsupported single tenant database type: " + databaseType);
      };
    }
    return new Tenant(databaseType, normalizedValidated);
  }

  public DatabaseType databaseType() {
    return databaseType;
  }

  /** Get the tenant id, this is a non-null String. */
  @Override
  public String toString() {
    return tenantId;
  }

  /** Hashed on the combined values of {@link #databaseType} and {@link #tenantId}. */
  @Override
  public int hashCode() {
    return Objects.hash(databaseType, tenantId);
  }

  /**
   * Equals based on the combined values of {@link #databaseType} and a CASE INSENSITIVE {@link
   * #tenantId}.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof Tenant that)) {
      return false;
    }
    return Objects.equals(databaseType, that.databaseType)
        && Objects.equals(tenantId, that.tenantId);
  }

  @Override
  public Recordable.DataRecorder recordTo(Recordable.DataRecorder dataRecorder) {
    return dataRecorder.append("tenantId", tenantId).append("databaseType", databaseType);
  }
}
