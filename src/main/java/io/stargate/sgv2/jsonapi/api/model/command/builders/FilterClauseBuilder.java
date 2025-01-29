package io.stargate.sgv2.jsonapi.api.model.command.builders;

import static io.stargate.sgv2.jsonapi.config.constants.DocumentConstants.Fields.DOC_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl.CollectionFilterClauseBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.impl.TableFilterClauseBuilder;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.collections.JsonExtensionType;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.math.BigDecimal;
import java.util.*;

/**
 * Object for converting {@link JsonNode} (from {@link FilterSpec}) into {@link FilterClause}.
 * Process will validate structure of the JSON, and also validate values of the filter operations.
 *
 * <p>TIDY: this class has a lot of string constants for filter operations that we have defined as
 * constants elsewhere
 */
public abstract class FilterClauseBuilder<T extends SchemaObject> {
  protected final T schema;

  protected FilterClauseBuilder(T schema) {
    this.schema = schema;
  }

  public static FilterClauseBuilder builderFor(SchemaObject schema) {
    return switch (schema) {
      case CollectionSchemaObject collection -> new CollectionFilterClauseBuilder(collection);
      case TableSchemaObject table -> new TableFilterClauseBuilder(table);
      default ->
          throw new UnsupportedOperationException(
              String.format(
                  "Unsupported schema object class for `FilterClauseBuilder`: %s",
                  schema.getClass()));
    };
  }

  public FilterClause build(OperationsConfig operationsConfig, JsonNode filterNode) {
    if (filterNode == null) {
      return null;
    }
    if (!filterNode.isObject()) {
      // JSON `null`s are ok though
      if (filterNode.isNull()) {
        return null;
      }
      throw ErrorCodeV1.UNSUPPORTED_FILTER_DATA_TYPE.toApiException();
    }

    // implicit and
    LogicalExpression implicitAnd = LogicalExpression.and();
    populateExpression(implicitAnd, filterNode);
    validateExpression(operationsConfig, implicitAnd);

    return buildAndValidate(implicitAnd);
  }

  // // // Abstract methods for sub-classes to implement

  protected abstract FilterClause buildAndValidate(LogicalExpression implicitAnd);

  // // // Construction of LogicalExpression from JSON

  private void populateExpression(LogicalExpression logicalExpression, JsonNode node) {
    if (logicalExpression == null) {
      return;
    }
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.fields();
      while (fieldsIterator.hasNext()) {
        Map.Entry<String, JsonNode> entry = fieldsIterator.next();
        populateExpression(logicalExpression, entry);
      }
    } else if (node.isArray()) {
      ArrayNode arrayNode = (ArrayNode) node;
      for (JsonNode next : arrayNode) {
        if (!next.isObject()) {
          // nodes in $and/$or array must be objects
          throw ErrorCodeV1.UNSUPPORTED_FILTER_DATA_TYPE.toApiException(
              "Unsupported NodeType %s in $%s",
              next.getNodeType(), logicalExpression.getLogicalRelation());
        }
        populateExpression(logicalExpression, next);
      }
    } else {
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "Cannot filter on '%s' field using operator '$eq': only '$exists' is supported",
          DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
    }
  }

  private void populateExpression(
      LogicalExpression logicalExpression, Map.Entry<String, JsonNode> entry) {
    if (entry.getValue().isObject()) {
      if (entry.getKey().equals("$not")) {
        LogicalExpression innerLogicalExpression = LogicalExpression.not();
        populateExpression(innerLogicalExpression, entry.getValue());
        logicalExpression.addLogicalExpression(innerLogicalExpression);
      } else {
        logicalExpression.addComparisonExpressions(createComparisonExpressionList(entry));
      }
      // inside of this entry, only implicit and, no explicit $and/$or
    } else if (entry.getValue().isArray()) {
      LogicalExpression innerLogicalExpression = null;
      switch (entry.getKey()) {
        case "$and":
          innerLogicalExpression = LogicalExpression.and();
          break;
        case "$or":
          innerLogicalExpression = LogicalExpression.or();
          break;
        case DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD:
        case DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD:
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "Cannot filter on '%s' field using operator '$eq': only '$exists' is supported",
              entry.getKey());
        default:
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "Cannot filter on '%s' by array type", entry.getKey());
      }
      ArrayNode arrayNode = (ArrayNode) entry.getValue();
      for (JsonNode next : arrayNode) {
        populateExpression(innerLogicalExpression, next);
      }
      logicalExpression.addLogicalExpression(innerLogicalExpression);
    } else {
      // the key should match pattern
      if (!DocumentConstants.Fields.VALID_PATH_PATTERN.matcher(entry.getKey()).matches()) {
        throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
            "filter clause path ('%s') contains character(s) not allowed", entry.getKey());
      }
      logicalExpression.addComparisonExpressions(
          List.of(
              ComparisonExpression.eq(
                  entry.getKey(), jsonNodeValue(entry.getKey(), entry.getValue()))));
    }
  }

  /**
   * The filter clause is entry will have field path as key and object type as value. The value can
   * have multiple operator and condition values.
   *
   * <p>Eg 1: {"field" : {"$eq" : "value"}}
   *
   * <p>Eg 2: {"field" : {"$gt" : 10, "$lt" : 50}}
   *
   * @param entry
   * @return
   */
  private List<ComparisonExpression> createComparisonExpressionList(
      Map.Entry<String, JsonNode> entry) {
    final List<ComparisonExpression> comparisonExpressionList = new ArrayList<>();
    final Iterator<Map.Entry<String, JsonNode>> fields = entry.getValue().fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> updateField = fields.next();
      final String updateKey = updateField.getKey();
      FilterOperator operator =
          FilterOperator.FilterOperatorUtils.findComparisonOperator(updateKey);

      // If assumed filter not found, may be JSON Extension value like "$date" or "$uuid";
      // or may be full Object to match
      if (operator == null) {
        JsonExtensionType etype = JsonUtil.findJsonExtensionType(updateKey);
        if ((etype == null) && updateKey.startsWith("$")) {
          throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(updateKey);
        }
        if (!DocumentConstants.Fields.VALID_PATH_PATTERN.matcher(entry.getKey()).matches()) {
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "filter clause path ('%s') contains character(s) not allowed", entry.getKey());
        }
        // JSON Extension type needs to be explicitly handled:
        Object value;
        if (etype != null) {
          if (entry.getKey().equals(DOC_ID)) {
            value = DocumentId.fromJson(entry.getValue());
          } else {
            value = JsonUtil.extractExtendedValue(etype, updateField);
          }
        } else {
          // Otherwise we have a full JSON Object to match:
          value = jsonNodeValue(entry.getKey(), entry.getValue());
        }
        comparisonExpressionList.add(ComparisonExpression.eq(entry.getKey(), value));
        return comparisonExpressionList;
      }

      // if the key does not match pattern or the entry is not ($vector and $exist operator)
      // combination, throw error
      if (!(DocumentConstants.Fields.VALID_PATH_PATTERN.matcher(entry.getKey()).matches()
          || (entry.getKey().equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)
              && updateField.getKey().equals("$exists"))
          || (entry.getKey().equals(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)
              && updateField.getKey().equals("$exists")))) {
        throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
            "filter clause path ('%s') contains character(s) not allowed", entry.getKey());
      }
      JsonNode value = updateField.getValue();
      Object valueObject = jsonNodeValue(entry.getKey(), value);
      if (operator == ValueComparisonOperator.GT
          || operator == ValueComparisonOperator.GTE
          || operator == ValueComparisonOperator.LT
          || operator == ValueComparisonOperator.LTE) {
        // Note, added 'valueObject instanceof String || valueObject instanceof Boolean', this is to
        // unblock some table filter against non-numeric column
        // e.g. {"event_date": {"$gt": "2024-09-24"}}, {"is_active": {"$gt": true}},
        // {"name":{"$gt":"Tim"}}
        // Also, for collection path, this will allow comparison filter against collection maps
        // query_bool_values and query_text_values
        if (!(valueObject instanceof Date
            || valueObject instanceof String
            || valueObject instanceof Boolean
            || valueObject instanceof BigDecimal
            || (valueObject instanceof DocumentId && (value.isObject() || value.isNumber())))) {
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "%s operator must have `DATE` or `NUMBER` or `TEXT` or `BOOLEAN` value",
              operator.getOperator());
        }
      }
      ComparisonExpression expression =
          new ComparisonExpression(entry.getKey(), new ArrayList<>(), null);
      expression.add(operator, valueObject);
      comparisonExpressionList.add(expression);
    }
    return comparisonExpressionList;
  }

  /**
   * Method to parse each filter clause and return node value.
   *
   * @param path - If the path is _id, then the value is resolved as DocumentId
   * @param node - JsonNode which has the operand value of a filter clause
   * @return
   */
  private Object jsonNodeValue(String path, JsonNode node) {
    // If the path is _id, then the value is resolved as DocumentId and Array type handled for `$in`
    // operator in filter
    if (path.equals(DOC_ID)) {
      if (node.getNodeType() == JsonNodeType.ARRAY) {
        ArrayNode arrayNode = (ArrayNode) node;
        List<Object> arrayVals = new ArrayList<>(arrayNode.size());
        for (JsonNode element : arrayNode) {
          arrayVals.add(jsonNodeValue(path, element));
        }
        return arrayVals;
      } else {
        return DocumentId.fromJson(node);
      }
    }
    return jsonNodeValue(node);
  }

  /**
   * Method to parse each filter clause and return node value. Called recursively in case of array
   * and object json types.
   *
   * @param node
   * @return
   */
  private Object jsonNodeValue(JsonNode node) {
    switch (node.getNodeType()) {
      case BOOLEAN:
        return node.booleanValue();
      case NUMBER:
        return node.decimalValue();
      case STRING:
        return node.textValue();
      case NULL:
        return null;
      case ARRAY:
        {
          ArrayNode arrayNode = (ArrayNode) node;
          List<Object> arrayVals = new ArrayList<>(arrayNode.size());
          for (JsonNode element : arrayNode) {
            arrayVals.add(jsonNodeValue(element));
          }
          return arrayVals;
        }
      case OBJECT:
        {
          if (JsonUtil.looksLikeEJsonValue(node)) { // means it's a single-entry Map, key
            JsonExtensionType etype = JsonUtil.findJsonExtensionType(node);
            if (etype == JsonExtensionType.EJSON_DATE) {
              JsonNode value = node.iterator().next();
              if (value.isIntegralNumber() && value.canConvertToLong()) {
                return new Date(value.longValue());
              }
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$date value has to be sent as epoch time");
            } else if (etype != null) {
              // This will convert to Java value if valid value; we'll just convert back to String
              // since all non-Date JSON extension values are indexed as Constants
              Object evalue = JsonUtil.extractExtendedValue(etype, node);
              return evalue.toString();
            } else {
              // handle an invalid filter use case:
              // { "address": { "street": { "$xx": xxx } } }
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "Invalid use of %s operator", node.fieldNames().next());
            }
          } else {
            ObjectNode objectNode = (ObjectNode) node;
            Map<String, Object> values = new LinkedHashMap<>(objectNode.size());
            final Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
              final Map.Entry<String, JsonNode> nextField = fields.next();
              values.put(nextField.getKey(), jsonNodeValue(nextField.getValue()));
            }
            return values;
          }
        }
      default:
        throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
            "Unsupported NodeType %s", node.getNodeType());
    }
  }

  // // // Validation of LogicalExpressions before building FilterClause

  private void validateExpression(
      OperationsConfig operationsConfig, LogicalExpression logicalExpression) {
    if (logicalExpression.getTotalIdComparisonExpressionCount() > 1) {
      throw ErrorCodeV1.FILTER_MULTIPLE_ID_FILTER.toApiException();
    }
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      validateExpression(operationsConfig, subLogicalExpression);
    }
    for (ComparisonExpression subComparisonExpression : logicalExpression.comparisonExpressions) {
      subComparisonExpression
          .getFilterOperations()
          .forEach(
              operation ->
                  validateExpression(
                      operationsConfig,
                      subComparisonExpression.getPath(),
                      operation,
                      logicalExpression.getLogicalRelation()));
    }
  }

  private void validateExpression(
      OperationsConfig operationsConfig,
      String path,
      FilterOperation<?> filterOperation,
      LogicalExpression.LogicalOperator fromLogicalRelation) {
    if (fromLogicalRelation.equals(LogicalExpression.LogicalOperator.OR)
        && path.equals(DocumentConstants.Fields.DOC_ID)) {
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "Cannot filter on '%s' field within '%s', ID field can not be used with $or operator",
          DocumentConstants.Fields.DOC_ID, LogicalExpression.LogicalOperator.OR.getOperator());
    }

    if (filterOperation.operator() instanceof ValueComparisonOperator valueComparisonOperator) {
      switch (valueComparisonOperator) {
        case IN -> {
          if (filterOperation.operand().value() instanceof List<?> list) {
            if (list.size() > operationsConfig.maxInOperatorValueSize()) {
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$in operator must have at most %d values",
                  operationsConfig.maxInOperatorValueSize());
            }
          } else {
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "$in operator must have `ARRAY`");
          }
        }
        case NIN -> {
          if (filterOperation.operand().value() instanceof List<?> list) {
            if (list.size() > operationsConfig.maxInOperatorValueSize()) {
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$nin operator must have at most %d values",
                  operationsConfig.maxInOperatorValueSize());
            }
          } else {
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "$nin operator must have `ARRAY`");
          }
        }
      }
    }

    if (filterOperation.operator() instanceof ElementComparisonOperator elementComparisonOperator) {
      switch (elementComparisonOperator) {
        case EXISTS:
          if (!(filterOperation.operand().value() instanceof Boolean)) {
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "$exists operator must have `BOOLEAN`");
          }
          break;
      }
    }

    if (filterOperation.operator() instanceof ArrayComparisonOperator arrayComparisonOperator) {
      switch (arrayComparisonOperator) {
        case ALL:
          if (filterOperation.operand().value() instanceof List<?> list) {
            if (list.isEmpty()) {
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$all operator must have at least one value");
            }
          } else {
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "$all operator must have `ARRAY` value");
          }
          break;
        case SIZE:
          if (filterOperation.operand().value() instanceof BigDecimal i) {
            if (i.intValue() < 0) {
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$size operator must have integer value >= 0");
            }
            // Check if the value is an integer by comparing its scale.
            if (i.stripTrailingZeros().scale() > 0) {
              throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                  "$size operator must have an integer value");
            }
          } else {
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "$size operator must have integer");
          }
          break;
      }
    }
  }
}
