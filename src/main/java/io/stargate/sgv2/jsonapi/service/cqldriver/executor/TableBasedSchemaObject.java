package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.api.request.tenant.Tenant;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectType;
import java.util.Objects;

public abstract class TableBasedSchemaObject extends SchemaObject {

  private final TableMetadata tableMetadata;

  protected TableBasedSchemaObject(
      SchemaObjectType type, Tenant tenant, TableMetadata tableMetadata) {
    super(type, SchemaObjectIdentifier.fromTableMetadata(type, tenant, tableMetadata));

    this.tableMetadata = Objects.requireNonNull(tableMetadata, "tableMetadata must not be null");
  }

  /**
   * For use by the CollectionSchemaObject, for old test that do not have TableMetadata
   * Use other ctor in prod code.
   */
  @VisibleForTesting
  protected TableBasedSchemaObject(
      SchemaObjectType expectedType,
      SchemaObjectIdentifier schemaObjectIdentifier,
      TableMetadata tableMetadata) {
    super(expectedType, schemaObjectIdentifier);

    this.tableMetadata = tableMetadata;
  }

  public CqlIdentifier keyspaceName() {
    return tableMetadata.getKeyspace();
  }

  public CqlIdentifier tableName() {
    return tableMetadata.getName();
  }

  public TableMetadata tableMetadata() {
    return tableMetadata;
  }
}
