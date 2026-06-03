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
 * <p>A tenant is identified by a non-null String tenant id, normalised and validated with {@link
 * DatabaseType#normalizeValidateTenantId(String)} and lower-cased at construction. We assumed the
 * tenantID is something like a UUID, see {@link #equals(Object)}.
 *
 * <p><b>IMPORTANT:</b> Work with Tenant instances, rather than get the string tenant id.
 *
 * <p>Safe for multi threading.
 */
public class Tenant implements Recordable {

  // TenantID used when the DB is single-tenant.
  private static final String SINGLE_TENANT_ID = "SINGLE-TENANT";

  /**
   * Region value baked into the Cassandra (single-tenant) Tenant instance. Cassandra deployments
   * don't have a per-request region concept, so all Cassandra tenants report this fixed string.
   */
  public static final String CASSANDRA_REGION_DEFAULT = "CASSANDRA_REGION";

  /**
   * Fallback region value for multi-tenant databases (e.g. Astra) when the request URL doesn't
   * carry a parseable region segment. Downstream billing pipelines can detect and investigate these
   * events.
   */
  public static final String UNKNOWN_REGION = "UNKNOWN_REGION";

  private static final Tenant SINGLE_TENANT_CASSANDRA =
      new Tenant(DatabaseType.CASSANDRA, SINGLE_TENANT_ID, CASSANDRA_REGION_DEFAULT);

  private final DatabaseType databaseType;
  private final String tenantId;
  private final String region;

  private Tenant(DatabaseType databaseType, String tenantId, String region) {
    this.databaseType = Objects.requireNonNull(databaseType, "databaseType must not be null");
    this.tenantId = Objects.requireNonNull(tenantId, "tenantId must not be null");
    this.region = Objects.requireNonNull(region, "region must not be null");
  }

  /**
   * Factory method to create a Tenant instance with no explicit region (region defaults per
   * database type — Cassandra gets {@link #CASSANDRA_REGION_DEFAULT}, other types get {@link
   * #UNKNOWN_REGION}).
   */
  @VisibleForTesting
  public static Tenant create(DatabaseType databaseType, String tenantId) {
    return create(databaseType, tenantId, null);
  }

  /**
   * Factory method to create a Tenant instance.
   *
   * <p>For single-tenant database types (e.g. Cassandra), the {@code region} argument is ignored
   * and the type's built-in default is used. For multi-tenant types, a null/blank {@code region} is
   * replaced by {@link #UNKNOWN_REGION}.
   *
   * <p>Visible for testing, you should use the {@link TenantFactory} to create instances
   *
   * @param databaseType the type of database this tenant is for, must not be null.
   * @param tenantId the tenant id, must be null or blank for single-tenant. Otherwise, null is
   *     normalized, and validated to be non-blank.
   * @param region the deployment region for the tenant (e.g. {@code us-west-2}, {@code
   *     southcentralus}), or null to use the type's default.
   * @return a Tenant instance
   */
  @VisibleForTesting
  public static Tenant create(DatabaseType databaseType, String tenantId, String region) {
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
    var normalizedRegion = (region == null || region.isBlank()) ? UNKNOWN_REGION : region;
    return new Tenant(databaseType, normalizedValidated.toLowerCase(), normalizedRegion);
  }

  public DatabaseType databaseType() {
    return databaseType;
  }

  /** The deployment region for this tenant. Never null. */
  public String region() {
    return region;
  }

  /** Get the tenant id, this is a non-null String. */
  @Override
  public String toString() {
    return tenantId;
  }

  /**
   * Hashed on the combined values of {@link #databaseType} and {@link #tenantId}. {@link #region}
   * is intentionally excluded — see {@link #equals(Object)}.
   */
  @Override
  public int hashCode() {
    return Objects.hash(databaseType, tenantId);
  }

  /**
   * Equals based on the combined values of {@link #databaseType} and {@link #tenantId}.
   *
   * <p>{@link #region} is intentionally excluded: a tenant is the same logical tenant in every
   * region it is reached from. Tenant is used as part of {@code SchemaObjectIdentifier} which keys
   * the schema object cache; including region here would split that cache unnecessarily.
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
    return dataRecorder
        .append("tenantId", tenantId)
        .append("databaseType", databaseType)
        .append("region", region);
  }
}
