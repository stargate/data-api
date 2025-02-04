package io.stargate.sgv2.jsonapi.api.model.command.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.service.schema.tables.ApiIndexDef;

/**
 * Public HTTP API description of an index used with the <code>listIndexes</code> command.
 *
 * <p>The definition, the index options, is in the <code>DefinitionT</code> class.
 *
 * <p>See {@link ApiIndexDef#indexDesc()} for usage.
 *
 * <p>Does not include the command options, because we don't want them in the list indexes command.
 * Needs @JsonProperty annotation because it is not using bean accessor methods.
 */
@JsonPropertyOrder({
  TableDescConstants.IndexDesc.NAME,
  TableDescConstants.IndexDesc.DEFINITION,
  TableDescConstants.IndexDesc.INDEX_TYPE
})
public interface IndexDesc<DefinitionT extends IndexDefinitionDesc<?>> {

  @JsonProperty(TableDescConstants.IndexDesc.NAME)
  String name();

  @JsonProperty(TableDescConstants.IndexDesc.DEFINITION)
  DefinitionT definition();

  @JsonProperty(TableDescConstants.IndexDesc.INDEX_TYPE)
  String indexType();
}
