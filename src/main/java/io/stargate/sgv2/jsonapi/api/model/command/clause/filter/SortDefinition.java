package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.builders.SortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import java.util.List;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link SortClause}, and allows
 * for lazy deserialization into a {@link SortClause}.
 */
@Schema(
    type = SchemaType.OBJECT,
    implementation = SortClause.class,
    examples =
        """
                  {"user.age" : -1, "user.name" : 1}
                  """)
public class SortDefinition extends JsonDefinition<SortClause> {
  /**
   * Lazily deserialized {@link SortClause} from the JSON value. We need this due to existing
   * reliance on specific stateful instances of {@link SortClause}.
   */
  private SortClause sortClause;

  /**
   * To deserialize the whole JSON value, need to ensure DELEGATING mode (instead of PROPERTIES).
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public SortDefinition(JsonNode json) {
    super(json);
  }

  private SortDefinition(SortClause sortClause) {
    // We do not really provide actual JSON value, but need to pass something to the parent
    super(JsonNodeFactory.instance.nullNode());
    this.sortClause = sortClause;
  }

  /**
   * Alternate constructor used to "wrap" already constructed {@link SortClause} into a {@link
   * SortDefinition} instance. Used by Find-and-Rerank functionality to pass already resolved {@link
   * SortClause}s.
   *
   * @param sortClause Actual sort clause to be wrapped.
   */
  public static SortDefinition wrap(SortClause sortClause) {
    return new SortDefinition(sortClause);
  }

  /**
   * Convert the JSON value to a {@link SortClause} instance and cache it, so further calls will
   * return the same instance.
   */
  public SortClause build(CommandContext<?> ctx) {
    if (sortClause == null) {
      sortClause = SortClauseBuilder.builderFor(ctx.schemaObject()).build(json());
    }
    return sortClause;
  }

  /**
   * Helper method unfortunately needed to check if the sort clause is a vector search clause,
   * called from context where we do not have access to the {@link SortClause} yet.
   *
   * @return True if the Sort clause definition contains a vector search clause.
   */
  public boolean hasVsearchClause() {
    // We will either be wrapping the clause or have a json value:
    if (sortClause != null) {
      return sortClause.hasVsearchClause();
    }
    return json().has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
  }

  /**
   * Helper method needed for logging purposes, where caller does not have access to the {@link
   * CommandContext}.
   */
  public List<String> getSortExpressionPaths() {
    // We will either be wrapping the clause or have a json value:
    if (sortClause != null) {
      return sortClause.sortExpressions().stream().map(expr -> expr.getPath()).toList();
    }
    return json().properties().stream().map(entry -> entry.getKey()).toList();
  }
}
