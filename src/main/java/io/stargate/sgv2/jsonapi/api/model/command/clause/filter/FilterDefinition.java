package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import static io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider.type;
import static org.eclipse.microprofile.openapi.models.media.Schema.SchemaType.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import io.quarkus.runtime.annotations.RegisterForReflection;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.builders.FilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.schema.ApiSchemaProvider;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import java.util.Map;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link FilterClause}, and
 * allows for lazy deserialization into a {@link FilterClause}.
 */
@Schema(
    description =
        "A Data API filter object mapping document paths to values or operator expressions, such as $eq, $gt, $in, $and, and $or.",
    implementation = FilterDefinition.SchemaProvider.class,
    ref = FilterDefinition.SchemaProvider.REF,
    examples =
        """
       {"name": "Aaron", "country": {"$eq": "NZ"}, "age": {"$gt": 40}}
        """)
public class FilterDefinition extends JsonDefinition<FilterClause> {
  @Schema(name = SchemaProvider.NAME)
  @RegisterForReflection
  public static class SchemaProvider implements ApiSchemaProvider {
    public static final String NAME = "FilterDefinition";
    public static final String REF = "#/components/schemas/" + NAME;

    @Override
    public org.eclipse.microprofile.openapi.models.media.Schema schema() {
      return type(OBJECT, NULL)
          .description(FilterDefinition.class.getAnnotation(Schema.class).description())
          .additionalPropertiesBoolean(true)
          .addExample(Map.of("age", Map.of("$gt", 40)))
          .addExample(Map.of("name", "Aaron", "country", Map.of("$eq", "NZ")));
    }
  }

  /**
   * Lazily deserialized {@link FilterClause} from the JSON value. We need this due to existing
   * reliance on specific stateful instances of {@link FilterClause}.
   */
  private FilterClause filterClause;

  /**
   * To deserialize the whole JSON value, need to ensure DELEGATING mode (instead of PROPERTIES).
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public FilterDefinition(JsonNode json) {
    super(json);
  }

  /**
   * Convert the JSON value to a {@link FilterClause} instance and cache it, so further calls will
   * return the same instance.
   *
   * @param ctx The command context to resolve the filter clause.
   * @return The resolved filter clause.
   */
  @Override
  public FilterClause build(CommandContext<?> ctx) {
    if (filterClause == null) {
      filterClause =
          FilterClauseBuilder.builderFor(ctx.schemaObject())
              .build(ctx.config().get(OperationsConfig.class), json());
    }
    return filterClause;
  }
}
