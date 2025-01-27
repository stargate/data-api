package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.deserializers.FilterClauseDeserializer;
import java.util.Objects;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link FilterClause}, and
 * allows for lazy deserialization into a {@link FilterClause}.
 */
@Schema(
    type = SchemaType.OBJECT,
    implementation = FilterClause.class,
    example =
        """
                     {"name": "Aaron", "country": "US"}
                      """)
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
   * efficient re-use) so that future calls to this method will return the same instance without
   * re-processing.
   *
   * @param ctx Command context to use for deserialization
   * @return Valid deserialized {@link FilterClause}
   */
  public FilterClause toFilterClause(CommandContext<?> ctx) {
    Objects.requireNonNull(ctx, "CommandContext cannot be null");
    if (null == filterClause) {
      filterClause = deserializer.buildFilterClause(ctx, filterJson);
    }
    return filterClause;
  }
}
