package io.stargate.sgv2.jsonapi.service.schema;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToMessageString;
import static io.stargate.sgv2.jsonapi.util.StringUtil.normalizeOptionalString;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.logging.LoggingMDCContext;
import io.stargate.sgv2.jsonapi.util.recordable.Recordable;
import java.util.Objects;
import javax.annotation.Nullable;
import org.slf4j.MDC;

/**
 * Identifies a {@link SchemaObject} in the API, which can be a database, keyspace, collection, etc.
 *
 * <p>Schema objects are identified by their type, name, and importantly, the tenant they belong to.
 * So an identifier is unique and stable for all tenants in a single instance of the API.
 *
 * <p>Create using the factory methods, such as {@link #forDatabase(Tenant)} which validate the data
 * that is needed.
 *
 * <p>use {@link #fullName()} or {@link #toString()} to get a human-readable representation of the
 * identifier, such as <code>keyspace_name.table_name</code> <b>Note:</b> You should compare and
 * manages the identifiers as objects using the {@link #equals(Object)} and {@link #hashCode()}
 * methods, which include the tenant etc in the logic. Avoid comparing the individual fields, such
 * as {@link #tenant()}, {@link #keyspace()} or {@link #table()}.
 */
public class SchemaObjectIdentifier
    implements UnscopedSchemaObjectIdentifier, Recordable, LoggingMDCContext {

  private final SchemaObjectType type;
  private final Tenant tenant;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;

  private final String fullName;

  private SchemaObjectIdentifier(
      SchemaObjectType type, Tenant tenant, CqlIdentifier keyspace, CqlIdentifier table) {

    this.type = type;
    this.tenant = tenant;
    this.keyspace = keyspace;
    this.table = table;

    this.fullName =
        switch (type) {
          case DATABASE -> "db:" + tenant;
          case KEYSPACE -> cqlIdentifierToMessageString(keyspace);
            // Note, INDEX and UDT schemaType are not used currently
            // Added for syntax completeness
          case COLLECTION, TABLE, INDEX, UDT ->
              cqlIdentifierToMessageString(keyspace) + "." + cqlIdentifierToMessageString(table);
        };
  }

  /** Creates a {@link SchemaObjectIdentifier} for a database. */
  public static SchemaObjectIdentifier forDatabase(Tenant tenant) {

    checkTenantId(tenant);
    return new SchemaObjectIdentifier(SchemaObjectType.DATABASE, tenant, null, null);
  }

  /** Creates a {@link SchemaObjectIdentifier} for a keyspace. */
  public static SchemaObjectIdentifier forKeyspace(Tenant tenant, CqlIdentifier keyspace) {

    checkTenantId(tenant);
    checkKeyspaceName(keyspace);
    return new SchemaObjectIdentifier(SchemaObjectType.KEYSPACE, tenant, keyspace, null);
  }

  /** Creates a {@link SchemaObjectIdentifier} for a collection. */
  public static SchemaObjectIdentifier forCollection(
      Tenant tenant, CqlIdentifier keyspace, CqlIdentifier collection) {

    checkTenantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("collection", collection);
    return new SchemaObjectIdentifier(SchemaObjectType.COLLECTION, tenant, keyspace, collection);
  }

  /** Creates a {@link SchemaObjectIdentifier} for a table. */
  public static SchemaObjectIdentifier forTable(
      Tenant tenant, CqlIdentifier keyspace, CqlIdentifier table) {

    checkTenantId(tenant);
    checkKeyspaceName(keyspace);
    checkTableName("table", table);
    return new SchemaObjectIdentifier(SchemaObjectType.TABLE, tenant, keyspace, table);
  }

  /** Creates a {@link SchemaObjectIdentifier} using CQL TableMetadata to get the name parts. */
  public static SchemaObjectIdentifier fromTableMetadata(
      SchemaObjectType type, Tenant tenant, TableMetadata tableMetadata) {

    checkTenantId(tenant);
    Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");

    return switch (type) {
      case TABLE -> forTable(tenant, tableMetadata.getKeyspace(), tableMetadata.getName());
      case COLLECTION ->
          forCollection(tenant, tableMetadata.getKeyspace(), tableMetadata.getName());
      default ->
          throw new IllegalArgumentException(
              "fromTableMetadata() - Unsupported object type: " + type);
    };
  }

  public SchemaObjectType type() {
    return type;
  }

  /**
   * The full name of the schema object, this is also returned from {@link #toString()}.:
   *
   * <ul>
   *   <li>For a database: <code>db:tenant</code>
   *   <li>For a keyspace: <code>keyspace</code>
   *   <li>For a collection, table, or index: <code>keyspace.collection</code>
   * </ul>
   */
  public String fullName() {
    return fullName;
  }

  public Tenant tenant() {
    return tenant;
  }

  /** Gets the {@link SchemaObjectIdentifier} for the keyspace that contains this schema object. */
  public SchemaObjectIdentifier keyspaceIdentifier() {
    return forKeyspace(tenant, keyspace);
  }

  @Override
  public CqlIdentifier keyspace() {
    return keyspace;
  }

  @Nullable
  public CqlIdentifier table() {
    return table;
  }

  /** Same as {@link #table()} , part of the {@link UnscopedSchemaObjectIdentifier} interface. */
  @Override
  public CqlIdentifier objectName() {
    return table;
  }

  /** Tests if this identifier is from the same tenant AND keyspace as another identifier. */
  public boolean isSameKeyspace(SchemaObjectIdentifier other) {
    return Objects.equals(tenant, other.tenant) && Objects.equals(keyspace, other.keyspace);
  }

  private static void checkTenantId(Tenant tenant) {
    Objects.requireNonNull(tenant, "tenant name must not be null");
  }

  private static void checkKeyspaceName(CqlIdentifier keyspace) {
    Objects.requireNonNull(keyspace, "keyspace name must not be null");
    Preconditions.checkArgument(
        !keyspace.asInternal().isBlank(), "keyspace name must not be blank");
  }

  private static void checkTableName(String context, CqlIdentifier table) {
    Objects.requireNonNull(table, context + " name must not be null");
    Preconditions.checkArgument(!table.asInternal().isBlank(), context + " name must not be blank");
  }

  @Override
  public void addToMDC() {
    // NOTE: MUST stay as namespace for logging analysis
    // keyspace can be null for DatabaseSchemaObjectIdentifier
    MDC.put("namespace", normalizeOptionalString(keyspace == null ? null : keyspace.asInternal()));

    // NOTE: MUST stay as collection for logging analysis
    MDC.put("collection", normalizeOptionalString(table == null ? null : table.asInternal()));
  }

  @Override
  public void removeFromMDC() {
    MDC.remove("namespace");
    MDC.remove("collection");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SchemaObjectIdentifier that)) {
      return false;
    }
    return Objects.equals(type, that.type)
        && Objects.equals(tenant, that.tenant)
        && Objects.equals(keyspace, that.keyspace)
        && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, tenant, keyspace, table);
  }

  /** Gets the {@link #fullName()} */
  @Override
  public String toString() {
    return fullName();
  }

  @Override
  public DataRecorder recordTo(DataRecorder dataRecorder) {
    return dataRecorder
        .append("tenant", tenant)
        .append("type", type)
        .append("keyspace", keyspace)
        .append("table", table);
  }
}
