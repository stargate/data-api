package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
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
              columnValues.put(CqlIdentifier.fromCql(entry.getKey()), shredValue(entry.getValue()));
            });

    // get all the primary key values that are in the document, this does not check if all are
    // present, that happens
    // when we do the WriteableTableRow is validated.
    var primaryKeyValues =
        table.tableMetadata.getPrimaryKey().stream()
            .map(ColumnMetadata::getName)
            .map(columnValues::get)
            .toList();

    return new WriteableTableRow(new RowId(primaryKeyValues.toArray()), columnValues);
  }

  /**
   * Function that will convert a JSONNode value, e.g. '1' into the correct Java type expected when
   * processing tables, e.g. BigDecimal.
   *
   * <p>The types returned here are types that are expected by the {@link
   * io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry} so we know
   * how to convert them into the correct Java types expected by the CQL driver.
   *
   * <p>The main difference here is that we convert all numbers to BigDecimal, and then defer
   * conversion into the type defined by the CQL Column until we are building the CQL statement
   * (e.g. insert, or select) where we bind to the column in the table and use the codec to sort it
   * out.
   *
   * @param value
   * @return
   */
  public static Object shredValue(JsonNode value) {
    return switch (value.getNodeType()) {
      case NUMBER -> value.decimalValue();
      case STRING -> value.textValue();
      case BOOLEAN -> value.booleanValue();
      case NULL -> null;
      default -> throw new RuntimeException("Unsupported type");
    };
  }
}
