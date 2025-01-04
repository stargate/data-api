package io.stargate.sgv2.jsonapi.api.model.command.table;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.stargate.sgv2.jsonapi.api.model.command.table.definition.indexes.IndexDefinitionDesc;
import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;

/**
 * Top level description of an index, used in the create index commands when we want to describe
 * existing indexes with list indexes command.
 *
 * <p>Does not include the command options, because we don't want them in the list indexes command.
 * Needs @JsonProperty annotation because it is not using bean accessor methods.
 */
@JsonPropertyOrder({TableDescConstants.IndexDesc.NAME, TableDescConstants.IndexDesc.DEFINITION})
public interface IndexDesc<DefinitionT extends IndexDefinitionDesc<?>> {

  @JsonProperty
  String name();

  @JsonProperty
  String indexType();

  @JsonProperty
  DefinitionT definition();
}
