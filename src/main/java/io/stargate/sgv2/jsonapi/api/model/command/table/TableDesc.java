package io.stargate.sgv2.jsonapi.api.model.command.table;

import com.fasterxml.jackson.annotation.JsonIgnore;
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
public record TableDesc(
    @JsonIgnore SchemaDescSource schemaDescSource,
    @JsonProperty(TableDescConstants.TableDesc.NAME) String name,
    @JsonProperty(TableDescConstants.TableDesc.DEFINITION) TableDefinitionDesc definition)
    implements SchemaDescription {}
