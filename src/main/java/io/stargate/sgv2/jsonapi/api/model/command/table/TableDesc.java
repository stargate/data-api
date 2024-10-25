package io.stargate.sgv2.jsonapi.api.model.command.table;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * Object used to build the response for listTables command
 *
 * @param name
 * @param definition
 */
@JsonPropertyOrder({"name", "definition"})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableDesc(
    String name, io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDesc definition) {

  public static TableDesc from(
      CqlIdentifier name,
      io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDesc definition) {
    return new TableDesc(cqlIdentifierToJsonKey(name), definition);
  }
}
