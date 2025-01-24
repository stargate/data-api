package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link FilterClause}, as well
 * as lazily-resolving provider of fully resolved {@link FilterClause}.
 */
public class FilterSpec {
  private static final FilterClauseDeserializer deserializer = new FilterClauseDeserializer();

  private final JsonNode filterJson;

  private FilterClause filterClause;

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FilterSpec(JsonNode filterJson) {
    this.filterJson = filterJson;
  }

  public FilterClause toFilterClause(CommandContext<?> context) {
    if (null == filterClause) {
      // !!! TODO: Pass CommandContext
      filterClause = deserializer.deserialize(filterJson);
    }
    return filterClause;
  }
}
