package io.stargate.sgv2.jsonapi.service.shredding.tables;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.api.v1.metrics.JsonProcessingMetricsReporter;
import io.stargate.sgv2.jsonapi.config.DocumentLimitsConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/** AATON TODO shreds docs for rows */
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
  public WriteableTableRow shred(JsonNode document) {

    // HACK for now we assume the primary is a field called primary key.

    Object keyObject;
    try {
      keyObject = objectMapper.treeToValue(document.get("key"), Object.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    Map<CqlIdentifier, Object> columnValues = new HashMap<>();
    document
        .fields()
        .forEachRemaining(
            entry -> {
              // using fromCQL so it is case sensitive
              try {
                columnValues.put(
                    CqlIdentifier.fromCql(entry.getKey()),
                    objectMapper.treeToValue(entry.getValue(), Object.class));
              } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
              }
            });
    return new WriteableTableRow(new RowId(new Object[] {keyObject}), columnValues);
  }
}
