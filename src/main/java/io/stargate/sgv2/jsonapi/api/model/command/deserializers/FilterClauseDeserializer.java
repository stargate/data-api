package io.stargate.sgv2.jsonapi.api.model.command.deserializers;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.quarkus.logging.Log;
import io.smallrye.config.SmallRyeConfig;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.*;
import io.stargate.sgv2.jsonapi.config.OperationsConfig;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.util.JsonUtil;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.microprofile.config.ConfigProvider;

/** {@link StdDeserializer} for the {@link FilterClause}. */
public class FilterClauseDeserializer extends StdDeserializer<FilterClause> {
  private final OperationsConfig operationsConfig;

  public FilterClauseDeserializer() {
    super(FilterClause.class);
    SmallRyeConfig config = ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
    operationsConfig = config.getConfigMapping(OperationsConfig.class);
  }

  /**
   * {@inheritDoc} Filter clause can follow short-cut {"field" : "value"} instead of {"field" :
   * {"$eq" : "value"}}
   */
  @Override
  public FilterClause deserialize(
      JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
    JsonNode filterNode = deserializationContext.readTree(jsonParser);
    if (!filterNode.isObject()) throw new JsonApiException(ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE);
    //        Iterator<Map.Entry<String, JsonNode>> fieldIter = filterNode.fields();
    // implicit and
    Log.error("entry 111");
    LogicalExpression implicitAnd = LogicalExpression.and();
    populateExpression(implicitAnd, filterNode);
    Log.error("give me ~~~~~~ " + implicitAnd);
    ////        List<ComparisonExpression> expressionList = new ArrayList<>();
    //        while (fieldIter.hasNext()) {
    //            //single filter map
    //            Map.Entry<String, JsonNode> entry = fieldIter.next();
    //            Log.info("filter field ------ " + entry);
    //            // TODO: Does not handle logical expressions, they are out of scope
    //            JsonNode operatorExpression = entry.getValue();
    //
    //            if (operatorExpression.isObject()) {
    //                //if is object, then there will be no explicit $and,$or in it ???
    //                Log.info("filter field entry value ------ " + entry.getValue() + " --- is
    // object");
    //                implicitAnd.addComparisonExpression(createComparisonExpression(entry));
    ////                implicitAnd.add(createLogicalExpression(entry));
    ////                expressionList.add(createComparisonExpression(entry));
    //            } else if(operatorExpression.isArray()) {
    //                //if is array, then there may be nested $and,$or in it
    //                implicitAnd.addLogicalExpression();
    //            }else{
    //                implicitAnd.addComparisonExpression(ComparisonExpression.eq(
    //                        entry.getKey(), jsonNodeValue(entry.getKey(), entry.getValue())));
    ////                expressionList.add(
    ////                        ComparisonExpression.eq(
    ////                                entry.getKey(), jsonNodeValue(entry.getKey(),
    // entry.getValue())));
    //
    ////                Log.info(
    ////                        "filter field entry value ------ " + entry.getValue() + " --- is
    // array/string/???");
    ////                // @TODO: Need to add array value type to this
    //////        if(entry.getValue().isArray()){
    //////          //$and, $or
    //////          expressionList.add(constructNestedExpression(entry)){
    //////
    //////          }
    //////        }
    //
    //            }
    //        }
    //        validate(expressionList);
    //        Log.info("important expression List " + expressionList);
    //        Log.info("important FilterClause " + new FilterClause(expressionList));

    //        return new FilterClause(expressionList);
    return null;
  }

  private void populateExpression(LogicalExpression logicalExpression, JsonNode node) {
    Log.error("entry 222");
    if (node.isObject()) {
      Log.error("is Object !!! ");
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
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
              String.format(
                  "Unsupported NodeType %s in $%s",
                  next.getNodeType(), logicalExpression.getLogicalRelation()));
        }
        populateExpression(logicalExpression, next);
      }
    } else {
      Log.error("should never reach here");
    }
  }

  private void populateExpression(
      LogicalExpression logicalExpression, Map.Entry<String, JsonNode> entry) {
    Log.error("entry 333");
    if (entry.getValue().isObject()) {
      // inside of this entry, only implicit and, no explicit $and/$or
      logicalExpression.addComparisonExpression(createComparisonExpression(entry));
    } else if (entry.getValue().isArray()) {
      Log.error("entry 444 " + entry.getKey());
      LogicalExpression innerLogicalExpression =
          entry.getKey().equals("$and") ? LogicalExpression.and() : LogicalExpression.or();
      ArrayNode arrayNode = (ArrayNode) entry.getValue();
      for (JsonNode next : arrayNode) {
        populateExpression(innerLogicalExpression, next);
      }
      logicalExpression.addLogicalExpression(innerLogicalExpression);
    } else {
      logicalExpression.addComparisonExpression(
          ComparisonExpression.eq(entry.getKey(), jsonNodeValue(entry.getKey(), entry.getValue())));
    }
  }

  private void validate(List<ComparisonExpression> expressionList) {
    for (ComparisonExpression expression : expressionList) {
      expression.filterOperations().forEach(operation -> validate(expression.path(), operation));
    }
  }

  private void validate(String path, FilterOperation<?> filterOperation) {
    // First: $vector can only be used with $exists operator
    if (path.equals(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)
        && ElementComparisonOperator.EXISTS != filterOperation.operator()) {
      throw new JsonApiException(
          ErrorCode.INVALID_FILTER_EXPRESSION,
          String.format(
              "Cannot filter on '%s' field using operator '%s': only '$exists' is supported",
              DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD,
              filterOperation.operator().getOperator()));
    }
    if (filterOperation.operator() instanceof ValueComparisonOperator valueComparisonOperator) {
      switch (valueComparisonOperator) {
        case IN -> {
          if (filterOperation.operand().value() instanceof List<?> list) {
            if (list.size() > operationsConfig.defaultPageSize()) {
              throw new JsonApiException(
                  ErrorCode.INVALID_FILTER_EXPRESSION,
                  "$in operator must have at most "
                      + operationsConfig.maxInOperatorValueSize()
                      + " values");
            }
          } else {
            throw new JsonApiException(
                ErrorCode.INVALID_FILTER_EXPRESSION, "$in operator must have `ARRAY`");
          }
        }
      }
    }

    if (filterOperation.operator() instanceof ElementComparisonOperator elementComparisonOperator) {
      switch (elementComparisonOperator) {
        case EXISTS:
          if (filterOperation.operand().value() instanceof Boolean b) {
            if (!b)
              throw new JsonApiException(
                  ErrorCode.INVALID_FILTER_EXPRESSION, "$exists operator supports only true");
          } else {
            throw new JsonApiException(
                ErrorCode.INVALID_FILTER_EXPRESSION, "$exists operator must have `BOOLEAN`");
          }
          break;
      }
    }

    if (filterOperation.operator() instanceof ArrayComparisonOperator arrayComparisonOperator) {
      switch (arrayComparisonOperator) {
        case ALL:
          if (filterOperation.operand().value() instanceof List<?> list) {
            if (list.isEmpty()) {
              throw new JsonApiException(
                  ErrorCode.INVALID_FILTER_EXPRESSION,
                  "$all operator must have at least one value");
            }
          } else {
            throw new JsonApiException(
                ErrorCode.INVALID_FILTER_EXPRESSION, "$all operator must have `ARRAY` value");
          }
          break;
        case SIZE:
          if (filterOperation.operand().value() instanceof BigDecimal i) {
            if (i.intValue() < 0) {
              throw new JsonApiException(
                  ErrorCode.INVALID_FILTER_EXPRESSION,
                  "$size operator must have integer value >= 0");
            }
          } else {
            throw new JsonApiException(
                ErrorCode.INVALID_FILTER_EXPRESSION, "$size operator must have integer");
          }
          break;
      }
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
  private ComparisonExpression createComparisonExpression(Map.Entry<String, JsonNode> entry) {
    ComparisonExpression expression = new ComparisonExpression(entry.getKey(), new ArrayList<>());
    // Check if the value is EJson date and add filter expression for date filter
    final Iterator<Map.Entry<String, JsonNode>> fields = entry.getValue().fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> updateField = fields.next();
      FilterOperator operator = null;
      try {
        operator = FilterOperator.FilterOperatorUtils.getComparisonOperator(updateField.getKey());
      } catch (JsonApiException exception) {
        // If getComparisonOperator returns an exception, check for subdocument equality condition,
        // this will happen when shortcut is used "filter" : { "size" : { "w": 21, "h": 14} }
        if (updateField.getKey().startsWith("$")
            && entry.getValue().get(JsonUtil.EJSON_VALUE_KEY_DATE) == null) {
          throw exception;
        } else {
          return ComparisonExpression.eq(
              entry.getKey(), jsonNodeValue(entry.getKey(), entry.getValue()));
        }
      }
      JsonNode value = updateField.getValue();
      // @TODO: Need to add array and sub-document value type to this condition
      expression.add(operator, jsonNodeValue(entry.getKey(), value));
    }
    return expression;
  }

  /**
   * Method to parse each filter clause and return node value.
   *
   * @param path - If the path is _id, then the value is resolved as DocumentId
   * @param node - JsonNode which has the operand value of a filter clause
   * @return
   */
  private static Object jsonNodeValue(String path, JsonNode node) {
    // If the path is _id, then the value is resolved as DocumentId and Array type handled for `$in`
    // operator in filter
    if (path.equals(DocumentConstants.Fields.DOC_ID)) {
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
  private static Object jsonNodeValue(JsonNode node) {
    Log.info("json node value " + node.getNodeType());
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
          Log.error("here");
          ArrayNode arrayNode = (ArrayNode) node;
          List<Object> arrayVals = new ArrayList<>(arrayNode.size());
          for (JsonNode element : arrayNode) {
            arrayVals.add(jsonNodeValue(element));
          }
          return arrayVals;
        }
      case OBJECT:
        {
          if (JsonUtil.looksLikeEJsonValue(node)) {
            Log.info("yes");
            JsonNode value = node.get(JsonUtil.EJSON_VALUE_KEY_DATE);
            if (value != null) {
              Log.error("???!");

              if (value.isIntegralNumber() && value.canConvertToLong()) {
                Log.error("???!!");

                return new Date(value.longValue());
              } else {
                Log.error("???");
                throw new JsonApiException(
                    ErrorCode.INVALID_FILTER_EXPRESSION, "Date value has to be sent as epoch time");
              }
            }
          } else {
            Log.info("yes1");

            ObjectNode objectNode = (ObjectNode) node;
            Map<String, Object> values = new LinkedHashMap<>(objectNode.size());
            final Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()) {
              final Map.Entry<String, JsonNode> nextField = fields.next();
              values.put(nextField.getKey(), jsonNodeValue(nextField.getValue()));
            }
            Log.info("done");

            return values;
          }
        }
      default:
        Log.error("woaco " + node.getNodeType());
        throw new JsonApiException(
            ErrorCode.UNSUPPORTED_FILTER_DATA_TYPE,
            String.format("Unsupported NodeType %s", node.getNodeType()));
    }
  }
}
