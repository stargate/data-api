package io.stargate.sgv2.jsonapi.api.model.command.table;

import static io.stargate.sgv2.jsonapi.util.CqlIdentifierUtil.cqlIdentifierToJsonKey;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.TableDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;

/**
 * Object used to build the response for listTables command
 *
 * @param name
 * @param definition
 */
@JsonPropertyOrder({TableDescConstants.TableDesc.NAME, TableDescConstants.TableDesc.DEFINITION})
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TableDesc (
    @JsonProperty(TableDescConstants.TableDesc.NAME) String name,
    @JsonProperty(TableDescConstants.TableDesc.DEFINITION) TableDefinitionDesc definition)
    implements SchemaDescription {

  public static TableDesc from(CqlIdentifier name, TableDefinitionDesc definition) {
    return new TableDesc(cqlIdentifierToJsonKey(name), definition);
  }
}
