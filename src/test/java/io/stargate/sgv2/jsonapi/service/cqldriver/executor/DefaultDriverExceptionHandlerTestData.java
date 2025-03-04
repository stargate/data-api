package io.stargate.sgv2.jsonapi.service.cqldriver.executor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DefaultDriverExceptionHandlerTestData {

  public final DriverExceptionHandler DRIVER_HANDLER;

  public final TableSchemaObject TABLE_SCHEMA_OBJECT;

  public final CqlIdentifier KEYSPACE_NAME =
      CqlIdentifier.fromInternal("keyspace-" + System.currentTimeMillis());

  public final CqlIdentifier TABLE_NAME =
      CqlIdentifier.fromInternal("table-" + System.currentTimeMillis());

  public final SimpleStatement STATEMENT =
      SimpleStatement.newInstance("SELECT * FROM " + TABLE_NAME.asCql(true) + " WHERE x=?;", 1);

  public DefaultDriverExceptionHandlerTestData() {

    // Its just as easy to create the table metadata from the driver.
    var tableMetadata =
        new DefaultTableMetadata(
            KEYSPACE_NAME,
            TABLE_NAME,
            UUID.randomUUID(),
            false,
            false,
            List.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            Map.of());
    TABLE_SCHEMA_OBJECT = TableSchemaObject.from(tableMetadata, new ObjectMapper());

    DRIVER_HANDLER = new DefaultDriverExceptionHandler<>(TABLE_SCHEMA_OBJECT, STATEMENT);
  }
}
