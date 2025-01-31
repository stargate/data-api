package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
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
       {"name": "Aaron", "country": {"$eq": "NZ"}, "age": {"$gt": 40}}
        """)
public class FilterSpec extends JsonDefinition {
  /**
   * Lazily deserialized {@link FilterClause} from the JSON value. We need this due to existing
   * reliance on specific stateful instances of {@link FilterClause}.
   */
  private FilterClause filterClause;

  /**
   * To deserialize the whole JSON value, need to ensure DELEGATING mode (instead of PROPERTIES).
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FilterSpec(JsonNode json) {
    super(json);
  }

  /**
   * Convert the JSON value to a {@link FilterClause} instance and cache it, so further calls will
   * return the same instance.
   *
   * @param ctx The command context to resolve the filter clause.
   * @return The resolved filter clause.
   */
  public FilterClause toFilterClause(CommandContext<?> ctx) {
    if (filterClause == null) {
      filterClause =
          FilterClauseBuilder.builderFor(ctx.schemaObject()).build(ctx.operationsConfig(), json());
    }
    return filterClause;
  }
}
