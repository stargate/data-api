package io.stargate.sgv2.jsonapi.api.model.command.clause.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.stargate.sgv2.jsonapi.api.model.command.JsonDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.builders.SortClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

/**
 * Intermediate lightly-processed container for JSON that specifies a {@link SortClause}, and allows
 * for lazy deserialization into a {@link SortClause}.
 */
@Schema(
    type = SchemaType.OBJECT,
    implementation = SortClause.class,
    example =
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
   *
   * @param schema Collection or Table for the current command.
   * @return The resolved filter clause.
   */
  public SortClause toSortClause(SchemaObject schema) {
    if (sortClause == null) {
      sortClause = SortClauseBuilder.builderFor(schema).build(json());
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
    return json().has(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
  }
}
