package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
import java.util.Objects;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link FilterClause}, as well
 * as lazily-resolving provider of fully resolved {@link FilterClause}.
 */
public class FilterSpec {
  private static final FilterClauseDeserializer deserializer = new FilterClauseDeserializer();

  /** The JSON value that specifies the filter clause. */
  private final JsonNode filterJson;

  /** Lazily built POJO representation of the filter clause. */
  private FilterClause filterClause;

  /**
   * To deserialize the whole JSON value, need to ensure DELEGATING mode (instead of PROPERTIES).
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FilterSpec(JsonNode filterJson) {
    this.filterJson = filterJson;
  }

  /**
   * Accessor for getting fully processed {@link FilterClause}. This method will lazily process the
   * JSON specification to produce the {@link FilterClause}, and then hold on to it (to allow
   * efficient re-use).
   *
   * @param ctx Command context to use for deserialization
   * @return Fully processed {@link FilterClause}
   */
  public FilterClause toFilterClause(CommandContext<?> ctx) {
    Objects.requireNonNull(ctx, "CommandContext cannot be null");
    if (null == filterClause) {
      // !!! TODO: Pass CommandContext
      filterClause = deserializer.buildFilterClause(ctx, filterJson);
    }
    return filterClause;
  }
}
