package io.stargate.sgv2.jsonapi.api.model.command.builders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
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
 * Object for converting {@link JsonNode} (from {@link FilterDefinition}) into {@link FilterClause}.
 * Process will validate structure of the JSON, and also validate values of the filter operations.
 *
 * <p>TIDY: this class has a lot of string constants for filter operations that we have defined as
 * constants elsewhere
 */
public abstract class FilterClauseBuilder<T extends SchemaObject> {
  protected final T schema;

  protected FilterClauseBuilder(T schema) {
    this.schema = Objects.requireNonNull(schema);
  }

  public static FilterClauseBuilder<?> builderFor(SchemaObject schema) {
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
    if (filterNode == null || filterNode.isNull()) {
      return FilterClause.empty();
    }
    if (!filterNode.isObject()) {
      throw ErrorCodeV1.UNSUPPORTED_FILTER_DATA_TYPE.toApiException();
    }

    // implicit and
    LogicalExpression implicitAnd = LogicalExpression.and();
    populateExpression(implicitAnd, filterNode);
    validateExpression(operationsConfig, implicitAnd);
    invertExpression(implicitAnd);

    // Could push down but for now seems like reasonable place to check
    final int totalExprCount = implicitAnd.getTotalComparisonExpressionCount();
    if (totalExprCount > operationsConfig.maxFilterObjectProperties()) {
      throw ErrorCodeV1.FILTER_FIELDS_LIMIT_VIOLATION.toApiException(
          "filter has %d fields, exceeds maximum allowed %s",
          totalExprCount, operationsConfig.maxFilterObjectProperties());
    }

    return validateAndBuild(implicitAnd);
  }

  // // // Abstract methods for sub-classes to implement

  /**
   * Method that will construct proper typed {@link FilterClause} and performance schema-dependent
   * validation before returning it.
   *
   * @param expression Root expression (implicit AND) to build the filter clause from
   * @return Built and validated filter clause
   */
  protected abstract FilterClause validateAndBuild(LogicalExpression expression);

  /**
   * Method for checking if the path refer to the document ID field: concept that only exists for
   * Collections. Used for doc-id specific construction and validation of constraints.
   */
  protected abstract boolean isDocId(String path);

  /**
   * Method to build the list of ComparisonExpression from a single path entry. Collection and Table
   * will have different implementations of this method.
   *
   * <p>E.G.
   *
   * <ul>
   *   <li><code>{"name": {"$eq" : "Tim"}}</code>
   *   <li><code>{"name": {"$gt" : 10, "$lt" : 50}}</code>
   *   <li><code>
   *       {"listColumn": {"$in": ["listValue1", "listValue2"], "$nin": ["listValue3", "listValue4"]}}
   *       </code> ...
   * </ul>
   */
  protected abstract List<ComparisonExpression> buildFromPathEntry(
      Map.Entry<String, JsonNode> entry);

  // // // Construction of LogicalExpression from JSON

  private void populateExpression(LogicalExpression logicalExpression, JsonNode node) {
    if (logicalExpression == null) {
      return;
    }
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> fieldsIterator = node.properties().iterator();
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
              "Unsupported NodeType '%s' for $%s filter",
              JsonUtil.nodeTypeAsString(next), logicalExpression.getLogicalRelation());
        }
        populateExpression(logicalExpression, next);
      }
    } else {
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "Cannot filter on '%s' field using operator $eq: only $exists is supported",
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
        logicalExpression.addComparisonExpressions(buildFromPathEntry(entry));
      }
      // inside of this entry, only implicit and, no explicit $and/$or
    } else if (entry.getValue().isArray()) {
      LogicalExpression innerLogicalExpression =
          switch (entry.getKey()) {
            case "$and" -> LogicalExpression.and();
            case "$or" -> LogicalExpression.or();
            case DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
                    DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD ->
                // TODO: (21-Jul-2025) Should be refactored to CollectionFilterClauseBuilder as it
                // only applies to Collections
                throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                    "Cannot filter on '%s' field using operator $eq: only $exists is supported",
                    entry.getKey());
            default ->
                throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                    "Cannot filter on '%s' by array type", entry.getKey());
          };
      ArrayNode arrayNode = (ArrayNode) entry.getValue();
      for (JsonNode next : arrayNode) {
        populateExpression(innerLogicalExpression, next);
      }
      logicalExpression.addLogicalExpression(innerLogicalExpression);
    } else { // neither Array nor Object, simple implicit "$eq" comparison
      // TODO: (21-Jul-2025) Should be refactored to CollectionFilterClauseBuilder as it
      // only applies to Collections
      switch (entry.getKey()) {
        case DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
                DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD ->
            throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
                "Cannot filter on '%s' field using operator $eq: only $exists is supported",
                entry.getKey());
      }
      // the key should match pattern
      String key = validateFilterClausePath(entry.getKey(), ValueComparisonOperator.EQ);
      logicalExpression.addComparisonExpressions(
          List.of(ComparisonExpression.eq(key, jsonNodeValue(key, entry.getValue()))));
    }
  }

  /**
   * The common path shared by both {@link CollectionFilterClauseBuilder} and {@link
   * TableFilterClauseBuilder}. It takes a single path entry and creates a list of {@link
   * ComparisonExpression} from it. E.G.
   *
   * <ul>
   *   <li><code>{"name" : {"$eq" : "value"}}</code>
   *   <li><code>{"name" : {"$gt" : 10, "$lt" : 50}}</code>
   * </ul>
   */
  protected List<ComparisonExpression> buildFromPathEntryCommon(Map.Entry<String, JsonNode> entry) {

    final List<ComparisonExpression> comparisonExpressionList = new ArrayList<>();
    final Iterator<Map.Entry<String, JsonNode>> fields = entry.getValue().properties().iterator();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> updateField = fields.next();
      final String updateKey = updateField.getKey();
      FilterOperator operator = FilterOperators.findComparisonOperator(updateKey);

      // If assumed filter not found, may be JSON Extension value like "$date" or "$uuid";
      // or may be full Object to match
      if (operator == null) {
        JsonExtensionType etype = JsonUtil.findJsonExtensionType(updateKey);
        if ((etype == null) && updateKey.startsWith("$")) {
          throw ErrorCodeV1.UNSUPPORTED_FILTER_OPERATION.toApiException(updateKey);
        }
        String key = validateFilterClausePath(entry.getKey(), ValueComparisonOperator.EQ);
        // JSON Extension type needs to be explicitly handled:
        Object value;
        if (etype != null) {
          if (isDocId(key)) {
            value = DocumentId.fromJson(entry.getValue());
          } else {
            value = JsonUtil.extractExtendedValue(etype, updateField);
          }
        } else {
          // Otherwise we have a full JSON Object to match:
          value = jsonNodeValue(key, entry.getValue());
        }
        comparisonExpressionList.add(ComparisonExpression.eq(entry.getKey(), value));
        return comparisonExpressionList;
      }

      String entryKey = validateFilterClausePath(entry.getKey(), operator);
      JsonNode value = updateField.getValue();
      Object valueObject = jsonNodeValue(entryKey, value);
      if (operator == ValueComparisonOperator.GT
          || operator == ValueComparisonOperator.GTE
          || operator == ValueComparisonOperator.LT
          || operator == ValueComparisonOperator.LTE) {
        // Comparator GT/GTE/LT/LTE can apply to following value types:
        // For Tables, Data/String/Boolean/BigDecimal
        // For Collections, Data/String/Boolean/BigDecimal and
        // DocumentID(Date/String/Boolean/BigDecimal)
        // E.G.
        // {"birthday": {"$gt": {"$date": 1672531200000}}}, {"name": {"$gt": "Tim"}}
        // {"is_active": {"$gt": true}}, {"age": {"$gt": 123}}
        // {"_id": {"$gt": {"$date": 1672531200000}}}, {"_id": {"$gt": "Tim"}}
        // {"_id": {"$gt": true}}, {"_id": {"$gt": 123}}
        if (!(valueObject instanceof Date
            || valueObject instanceof String
            || valueObject instanceof Boolean
            || valueObject instanceof BigDecimal
            || (valueObject instanceof DocumentId
                && (value.isObject()
                    || value.isTextual()
                    || value.isBoolean()
                    || value.isNumber())))) {
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "%s operator must have `DATE` or `NUMBER` or `TEXT` or `BOOLEAN` value",
              operator.getOperator());
        }
      } else if (operator == ValueComparisonOperator.MATCH) {
        // $match operator can only be used with String value
        if (!(valueObject instanceof String)) {
          throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
              "%s operator must have `String` value, was `%s`",
              operator.getOperator(), JsonUtil.nodeTypeAsString(value));
        }
      }

      ComparisonExpression expression = new ComparisonExpression(entryKey, new ArrayList<>(), null);
      expression.add(operator, valueObject);
      comparisonExpressionList.add(expression);
    }
    return comparisonExpressionList;
  }

  /**
   * Method to parse each filter clause and return node value.
   *
   * @param path - If the path refers to Document Id (see {@link #isDocId}, then the value is
   *     resolved as DocumentId
   * @param node - JsonNode which has the operand value of a filter clause
   * @return
   */
  protected Object jsonNodeValue(String path, JsonNode node) {
    // If the path is _id, then the value is resolved as DocumentId and Array type handled for `$in`
    // operator in filter
    if (isDocId(path)) {
      if (node.getNodeType() == JsonNodeType.ARRAY) {
        ArrayNode arrayNode = (ArrayNode) node;
        List<Object> arrayVals = new ArrayList<>(arrayNode.size());
        for (JsonNode element : arrayNode) {
          arrayVals.add(jsonNodeValue(path, element));
        }
        return arrayVals;
      }
      return DocumentId.fromJson(node);
    }
    return jsonNodeValue(node);
  }

  /**
   * Method to parse each filter clause and return node value. Called recursively in case of array
   * and object json types.
   */
  protected Object jsonNodeValue(JsonNode node) {
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
            final Iterator<Map.Entry<String, JsonNode>> fields = objectNode.properties().iterator();
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
    if (isDocId(path) && fromLogicalRelation.equals(LogicalExpression.LogicalOperator.OR)) {
      throw ErrorCodeV1.INVALID_FILTER_EXPRESSION.toApiException(
          "Cannot filter on '%s' field within '%s', ID field can not be used with $or operator",
          path, LogicalExpression.LogicalOperator.OR.getOperator());
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

  // // // Inversion of LogicalExpression right before constructing FilterClause

  LogicalExpression invertExpression(LogicalExpression logicalExpression) {
    if (logicalExpression != null) {
      invertLogicalExpression(logicalExpression, null);
    }
    return logicalExpression;
  }

  /**
   * Recursive method Invert the currentLogicalExpression, add its inverted children to
   * parentLogicalExpression examples: 1. {age=10, not{name=tim}} -> one comparisonExpression within
   * not, just revert it and add to ancestorLogicalExpression -> {age=10, name!=tim}
   *
   * <p>2. {age=10, not{or[address=Shanghai ,gender=male]}} -> one logicalExpression within not,
   * just revert it and add to ancestorLogicalExpression -> {age=10, and[address!=Shanghai
   * ,gender!=male]}
   *
   * <p>3. {age=10, not{name=tim, school=cmu}} -> two comparisonExpressions within not, revert them
   * and construct explicit or relation -> {age=10, or[name!=tim, school!=cmu]}
   *
   * <p>4. {age=10, not{or[address=Shanghai ,gender=male], name=tim}} -> one comparisonExpression
   * and one logicalExpression within not -> invert them and construct explicit or relation ->
   * {age=10, or[and[address!=Shanghai ,gender!=male], name!=tim]}
   *
   * <p>5. {age=10, not{or[address=Shanghai ,gender=male], and[color=yellow,height=175]}} ->
   * multiple logicalExpressions within not -> invert them and construct explicit or relation ->
   * {age=10, or[and[address!=Shanghai ,gender!=male], or[color!=yellow,height!=175]]}
   *
   * @param logicalExpression current logicalExpression
   * @param parentLogicalExpression parent logicalExpression
   */
  private void invertLogicalExpression(
      LogicalExpression logicalExpression, LogicalExpression parentLogicalExpression) {

    // create this temp list to avoid concurrentModification
    List<LogicalExpression> tempLogicalExpressions =
        new ArrayList<>(logicalExpression.logicalExpressions);

    for (LogicalExpression childLogicalExpression : tempLogicalExpressions) {
      invertLogicalExpression(childLogicalExpression, logicalExpression);
    }

    Iterator<LogicalExpression> iterator = logicalExpression.logicalExpressions.iterator();
    while (iterator.hasNext()) {
      LogicalExpression childLogicalExpression = iterator.next();
      if (childLogicalExpression.getLogicalRelation() == LogicalExpression.LogicalOperator.NOT) {
        iterator.remove();
      }
    }

    // Handle all the comparisonExpressions and logicalExpressions inside this $ånot
    if (logicalExpression.getLogicalRelation() == LogicalExpression.LogicalOperator.NOT) {
      // 1. recursively flip all the ComparisonExpression and LogicalExpression
      flip(logicalExpression);

      // 2. Different of situations here
      if (logicalExpression.comparisonExpressions.size() == 1
          && logicalExpression.logicalExpressions.isEmpty()) {
        // 2.1 only one comparisonExpression
        parentLogicalExpression.addComparisonExpressionsFlipped(
            List.of(logicalExpression.comparisonExpressions.get(0)));
      } else if (!logicalExpression.logicalExpressions.isEmpty()
          && logicalExpression.comparisonExpressions.isEmpty()) {
        // 2.2 only one logicalExpression
        logicalExpression.logicalExpressions.forEach(
            parentLogicalExpression::addLogicalExpressionFlipped);
      } else {
        // 2.3 multiple comparisonExpression
        // 2.4 multiple comparisonExpression and multiple logicalExpression
        // 2.5 multiple logicalExpression
        final LogicalExpression or = LogicalExpression.or();
        logicalExpression.comparisonExpressions.forEach(
            comparisonExpression ->
                or.addComparisonExpressionsFlipped(List.of(comparisonExpression)));
        logicalExpression.logicalExpressions.forEach(or::addLogicalExpressionFlipped);

        parentLogicalExpression.addLogicalExpressionFlipped(or);
      }

      // TODO: Is this needed? since we will remove the $not node no matter what
      // 3. clear the all the comparisonExpressions inside this $NOT
      logicalExpression.comparisonExpressions.clear();
    }
  }

  /**
   * Recursive method Invert the currentLogicalExpression recursively 1. AND -> OR 2. OR -> AND
   * 3.comparisonExpression -> opposite comparisonExpression
   *
   * @param logicalExpression logicalExpression
   */
  private void flip(LogicalExpression logicalExpression) {
    logicalExpression.setLogicalRelation(
        (LogicalExpression.LogicalOperator) logicalExpression.getLogicalRelation().invert());
    // flip child LogicalExpressions
    for (LogicalExpression childLogicalExpression : logicalExpression.logicalExpressions) {
      flip(childLogicalExpression);
    }
    // flip child ComparisonExpression
    for (ComparisonExpression childComparisonExpression : logicalExpression.comparisonExpressions) {
      childComparisonExpression.invert();
    }
  }

  /**
   * Method called to enforce the filter clause path to be valid. This method is called for each
   * path.
   *
   * @param path Path to be validated
   * @param operator FilterOperator that is used for this path
   * @return Path after validation - currently not changed
   */
  protected abstract String validateFilterClausePath(String path, FilterOperator operator);
}
