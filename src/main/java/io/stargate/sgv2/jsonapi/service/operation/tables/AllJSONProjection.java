package io.stargate.sgv2.jsonapi.service.operation.tables;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv2.jsonapi.service.operation.DocumentSource;
import org.apache.commons.lang3.NotImplementedException;

/**
 * POC implementation that represents a projection that includes all columns in the table, and does
 * a CQL select AS JSON
 */
public record AllJSONProjection(ObjectMapper objectMapper) implements OperationProjection {

  /**
   * POC implementation that selects all columns, and returns the result using CQL AS JSON
   *
   * @param select
   * @return
   */
  @Override
  public Select forSelect(SelectFrom select) {
    return select.json().all();
  }

  @Override
  public DocumentSource toDocument(Row row) {
    return (DocumentSource)
        () -> {
          try {
            return objectMapper.readTree(row.getString("[json]"));
          } catch (Exception e) {
            throw new NotImplementedException("Not implemented " + e.getMessage());
          }
        };
  }
}
