package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UnknownColumnException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/** AARON TODO shreds docs for rows */
@ApplicationScoped
public class RowShredder {

  private final ObjectMapper objectMapper;

  private final DocumentLimitsConfig documentLimits;

  private final JsonProcessingMetricsReporter jsonProcessingMetricsReporter;

  @Inject
  public RowShredder(
      ObjectMapper objectMapper,
      DocumentLimitsConfig documentLimits,
      JsonProcessingMetricsReporter jsonProcessingMetricsReporter) {
    this.objectMapper = objectMapper;
    this.documentLimits = documentLimits;
    this.jsonProcessingMetricsReporter = jsonProcessingMetricsReporter;
  }

  /**
   * Shreds the document to get it ready for the database, we need to know the table schema so we
   * can work out the primary key and the columns to insert
   *
   * @param document
   * @return
   */
  public WriteableTableRow shred(TableSchemaObject table, JsonNode document) {

    Map<CqlIdentifier, Object> columnValues = new HashMap<>();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              // using fromCQL so it is case sensitive

              Object value =
                  switch (entry.getValue().getNodeType()) {
                    case NUMBER -> entry.getValue().decimalValue();
                    case STRING -> entry.getValue().textValue();
                    case BOOLEAN -> entry.getValue().booleanValue();
                    case NULL -> null;
                    default -> throw new RuntimeException("Unsupported type");
                  };
              columnValues.put(CqlIdentifier.fromCql(entry.getKey()), value);
            });

    // the document should have been validated that all the fields present exist in the table
    // and that all the primary key fields on the table have been included in the document.
    var primaryKeyValues =
        table.tableMetadata.getPrimaryKey().stream()
            .map(ColumnMetadata::getName)
            .map(
                colIdentifier -> {
                  if (columnValues.containsKey(colIdentifier)) {
                    return columnValues.get(colIdentifier);
                  }
                  throw new UnknownColumnException(table.tableMetadata, colIdentifier);
                })
            .toList();

    return new WriteableTableRow(new RowId(primaryKeyValues.toArray()), columnValues);
  }
}
