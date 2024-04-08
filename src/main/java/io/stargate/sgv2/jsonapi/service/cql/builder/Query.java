package io.stargate.sgv2.jsonapi.service.cql.builder;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.util.List;

/**
 * @param cql The query string. It can contain anonymous placeholders identified by a question mark
 *     (?), or named placeholders prefixed by a column (:name).
 * @param values The values to fill the placeholders in the query string.
 * @param parameters The execution parameters for the query. (not supported)
 */
public record Query(String cql, List<Object> values, List<Object> parameters) {

  public SimpleStatement queryToStatement() {
    SimpleStatement simpleStatement = SimpleStatement.newInstance(cql);
    return simpleStatement.setPositionalValues(values);
  }
}
