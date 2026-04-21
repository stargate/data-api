package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.EJSONWrapper;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.SortDefinition;
import io.stargate.sgv2.jsonapi.api.model.command.clause.sort.SortClause;
import io.stargate.sgv2.jsonapi.exception.SortException;
import io.stargate.sgv2.jsonapi.service.schema.SchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.tables.TableSchemaObject;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.util.Map;
import java.util.Objects;

/**
 * Object for converting {@link JsonNode} (from {@link SortDefinition}) into {@link SortClause}.
 * Process will validate structure of the JSON, and also validate values of the sort expressions.
 */
public abstract class SortClauseBuilder<T extends SchemaObject> {
  protected final T schema;

  protected SortClauseBuilder(T schema) {
    this.schema = Objects.requireNonNull(schema);
  }

  public static SortClauseBuilder<?> builderFor(SchemaObject schema) {
    return switch (schema) {
      case CollectionSchemaObject collection -> new CollectionSortClauseBuilder(collection);
      case TableSchemaObject table -> new TableSortClauseBuilder(table);
      default ->
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported schema object class for `SortClauseBuilder`: %s",
                  schema.getClass()));
    };
  }

  public SortClause build(JsonNode node) {
    // if missing or null, return "empty" sort clause
    if (node.isMissingNode() || node.isNull()) {
      return SortClause.empty();
    }

    // otherwise, if it's not object throw exception
    if (!(node instanceof ObjectNode sortNode)) {
      throw SortException.Code.SORT_CLAUSE_INVALID.get(
          Map.of(
              "problem",
              "sort clause must be submitted as JSON Object, not %s"
                  .formatted(JsonUtil.nodeTypeAsString(node))));
    }
    return buildClauseFromDefinition(sortNode);
  }

  /**
   * Main method to build a {@link SortClause} from the given JSON definition; called after the
   * paths are validated.
   *
   * @param sortNode Full JSON definition of the sort clause
   * @return SortClause built from the definition
   */
  protected abstract SortClause buildClauseFromDefinition(ObjectNode sortNode);

  protected float[] tryDecodeBinaryVector(String path, JsonNode innerValue) {
    if (innerValue instanceof ObjectNode innerObject) {
      var ejsonWrapped = EJSONWrapper.maybeFrom(innerObject);
      if (ejsonWrapped == null || ejsonWrapped.type() != EJSONWrapper.EJSONType.BINARY) {
        throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
            Map.of(
                "path",
                path,
                "problem",
                "only binary vector object values are supported for sorting, not value: %s"
                    .formatted(innerValue.toString())));
      }
      try {
        return ejsonWrapped.getVectorValueForBinary();
      } catch (IllegalArgumentException | IllegalStateException e) {
        throw SortException.Code.SORT_CLAUSE_VALUE_INVALID.get(
            Map.of("path", path, "problem", e.getMessage()));
      }
    }
    return null;
  }
}
