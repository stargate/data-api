package io.stargate.sgv2.jsonapi.service.operation.tables;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableExtensions;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.VectorIndexProfileDefinition;
import io.stargate.sgv2.jsonapi.service.operation.SchemaDBTask;
import io.stargate.sgv2.jsonapi.service.operation.keyspaces.KeyspaceDriverExceptionHandler;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObjectIdentifier;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DropVectorIndexProfileDBTaskTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void buildsAlterTableExtensionsStatementForOwningTable() {
    var identifier = mock(SchemaObjectIdentifier.class);
    when(identifier.keyspace()).thenReturn(CqlIdentifier.fromInternal("my_ks"));
    var schemaObject = mock(KeyspaceSchemaObject.class);
    when(schemaObject.identifier()).thenReturn(identifier);

    var customProperties =
        TableExtensions.createCustomProperties(
            Map.of(),
            Map.of("kept_idx", new VectorIndexProfileDefinition("small-high-recall", Map.of())),
            MAPPER);

    var task =
        DropVectorIndexProfileDBTask.builder(schemaObject)
            .withSchemaRetryPolicy(new SchemaDBTask.SchemaRetryPolicy(1, Duration.ofMillis(1)))
            .withExceptionHandlerFactory(KeyspaceDriverExceptionHandler::new)
            .withTableName(CqlIdentifier.fromInternal("my_table"))
            .withCustomProperties(customProperties)
            .build();

    var query = task.buildStatement().getQuery();

    // ALTER TABLE on the owning table, in the schema object keyspace, updating extensions
    assertThat(query)
        .contains("ALTER TABLE")
        .contains("my_ks")
        .contains("my_table")
        .contains("extensions");
  }
}
