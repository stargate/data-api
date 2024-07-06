package io.stargate.sgv2.jsonapi.service.cql.builder;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Variable;
import com.datastax.oss.driver.api.core.data.CqlVector;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cql.ColumnUtils;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSettings;
import io.stargate.sgv2.jsonapi.service.cqldriver.serializer.CQLBindValues;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.builder.BuiltCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class QueryBuilder {
  private String keyspaceName;
  private String tableName;
  private boolean isInsert;
  private boolean isUpdate;
  private boolean isDelete;
  private boolean isSelect;
  private Integer limitInt;
  private String orderByAnn;
  private final List<QueryBuilder.FunctionCall> functionCalls = new ArrayList<>();

  /** The vectorValue used to compute similarityScore or process an ANN search */
  private CqlVector<Float> vectorValue;

  /** Column names for a SELECT or DELETE. */
  private final List<String> selection = new ArrayList<>();

  /** The where expression which contains conditions and logic operation for a SELECT or UPDATE. */
  private Expression<BuiltCondition> whereExpression = null;

  private static final String COUNT_FUNCTION_NAME = "COUNT";

  public void keyspace(String keyspace) {
    this.keyspaceName = keyspace;
  }

  public void table(String table) {
    this.tableName = table;
  }

  public QueryBuilder from(String keyspace, String table) {
    this.keyspaceName = keyspace;
    table(table);
    return this;
  }

  public QueryBuilder select() {
    isSelect = true;
    return this;
  }

  public QueryBuilder column(String... columns) {
    for (String c : columns) {
      column(c);
    }
    return this;
  }

  public QueryBuilder column(String column) {
    if (isSelect || isDelete) {
      selection.add(column);
    }
    return this;
  }

  public QueryBuilder count() {
    count(null);
    return this;
  }

  public QueryBuilder count(String columnName) {
    functionCalls.add(FunctionCall.count(columnName));
    return this;
  }

  public QueryBuilder as(String alias) {
    if (functionCalls.isEmpty()) {
      throw new IllegalStateException(
          "The as() method cannot be called without a preceding function call.");
    }
    // the alias is set for the last function call
    FunctionCall functionCall = functionCalls.get(functionCalls.size() - 1);
    functionCall.setAlias(alias);
    return this;
  }

  private void setWhereExpression(Expression<BuiltCondition> whereExpression) {
    this.whereExpression = whereExpression;
  }

  public QueryBuilder where(Expression<BuiltCondition> whereExpression) {
    if (whereExpression != null) {
      setWhereExpression(whereExpression);
    }
    return this;
  }

  public QueryBuilder limit(Integer limit) {
    this.limitInt = limit;
    return this;
  }

  public QueryBuilder limit() {
    this.limitInt = -1;
    return this;
  }

  public Query build() {
    if (isSelect) {
      return selectQuery();
    }
    throw ErrorCode.UNSUPPORTED_CQL_QUERY_TYPE.toApiException();
  }

  private Query selectQuery() {
    List<Object> values = new ArrayList<>();
    StringBuilder builder = new StringBuilder("SELECT ");
    // Data API has 3 sets of selection columns: DOCUMENT, SORTED_DOCUMENT, KEY
    if (selection.isEmpty() && functionCalls.isEmpty()) {
      builder.append('*');
    } else {
      builder.append(
          Stream.concat(
                  selection.stream().map(QueryBuilder::cqlName),
                  functionCalls.stream()
                      .map(functionCall -> formatFunctionCall(functionCall, values)))
              .collect(Collectors.joining(", ")));
    }
    builder.append(" FROM ").append(maybeQualify(tableName));

    appendWheres(builder, values);

    if (orderByAnn != null) {
      if (vectorValue == null) {
        throw ErrorCode.MISSING_VECTOR_VALUE.toApiException();
      }
      builder.append(" ORDER BY ").append(orderByAnn).append(" ANN OF ?");
      values.add(vectorValue);
    }

    if (limitInt != null) {
      builder.append(" LIMIT ").append(limitInt == -1 ? "?" : limitInt);
    }

    return new Query(builder.toString(), values);
  }

  private void appendWheres(StringBuilder builder, List<Object> values) {
    // Data API fully rely on Expression<BuildCondition> instead of List<BuildCondition>
    if (this.whereExpression != null) {
      appendConditions(this.whereExpression, " WHERE ", builder, values);
    }
  }

  private void appendConditions(
      Expression<BuiltCondition> whereExpression,
      String initialPrefix,
      StringBuilder builder,
      List<Object> values) {
    builder.append(initialPrefix);
    addExpressionCql(builder, whereExpression, values);
  }

  private void addExpressionCql(
      StringBuilder sb, Expression<BuiltCondition> outerExpression, List<Object> values) {
    List<Expression<BuiltCondition>> innerExpressions = outerExpression.getChildren();
    switch (outerExpression.getExprType()) {
      case "and" -> {
        // have parenthesis only when having more than one innerExpression
        if (innerExpressions.size() > 1) {
          sb.append("(");
        }
        for (int i = 0; i < innerExpressions.size(); i++) {
          addExpressionCql(sb, innerExpressions.get(i), values);
          if (i == innerExpressions.size() - 1) {
            break;
          }
          sb.append(" AND ");
        }
        if (innerExpressions.size() > 1) {
          sb.append(")");
        }
      }
      case "or" -> {
        // TODO: this code is basically a duplicate of the match because with OR rather than AND
        // have parenthesis only when having more than one innerExpression
        if (innerExpressions.size() > 1) {
          sb.append("(");
        }
        for (int i = 0; i < innerExpressions.size(); i++) {
          addExpressionCql(sb, innerExpressions.get(i), values);
          if (i == innerExpressions.size() - 1) {
            break;
          }
          sb.append(" OR ");
        }
        if (innerExpressions.size() > 1) {
          sb.append(")");
        }
      }
      // TODO OMG MAKE THIS AN ENUM or something more than a string !
      // there is a public Variable.EXPR_TYPE
      case "variable" -> {
        Variable<BuiltCondition> variable = (Variable) outerExpression;
        BuiltCondition condition = variable.getValue();
        condition.lhs.appendToBuilder(sb);
        condition.rhsTerm.appendPositionalValue(values);
        sb.append(" ").append(condition.predicate.toString()).append(" ?");
      }
      default ->
          throw new IllegalArgumentException(
              String.format("Unsupported expression type %s", outerExpression.getExprType()));
    }
  }

  private static String cqlName(String name) {
    return ColumnUtils.maybeQuote(name);
  }

  private String maybeQualify(String elementName) {
    if (keyspaceName == null) {
      return cqlName(elementName);
    } else {
      return cqlName(keyspaceName) + '.' + cqlName(elementName);
    }
  }

  /**
   * @param functionCall functionCall such as similarityScore
   * @param values values list to be populated
   * @return
   */
  private String formatFunctionCall(QueryBuilder.FunctionCall functionCall, List<Object> values) {
    StringBuilder builder = new StringBuilder();
    if (functionCall.getColumnName() == null
        && COUNT_FUNCTION_NAME.equals(functionCall.getFunctionName())) {
      // count function call and no column name
      builder.append(functionCall.getFunctionName()).append("(1)");
    } else {
      builder
          .append(functionCall.getFunctionName())
          .append('(')
          .append(cqlName(functionCall.getColumnName()));
      if (functionCall.isSimilarityFunction) {
        if (vectorValue == null) {
          throw ErrorCode.MISSING_VECTOR_VALUE.toApiException();
        }
        builder.append(", ").append('?');
        values.add(vectorValue);
      }
      builder.append(')');
    }
    if (functionCall.getAlias() != null) {
      builder.append(" AS ").append(cqlName(functionCall.getAlias()));
    }

    return builder.toString();
  }

  public QueryBuilder similarityFunction(
      String columnName, CollectionSettings.SimilarityFunction similarityFunction) {
    switch (similarityFunction) {
      case COSINE, UNDEFINED ->
          functionCalls.add(FunctionCall.similarityFunctionCall(columnName, "SIMILARITY_COSINE"));
      case EUCLIDEAN ->
          functionCalls.add(
              FunctionCall.similarityFunctionCall(columnName, "SIMILARITY_EUCLIDEAN"));
      case DOT_PRODUCT ->
          functionCalls.add(
              FunctionCall.similarityFunctionCall(columnName, "SIMILARITY_DOT_PRODUCT"));
      default ->
          throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
              ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage() + similarityFunction);
    }
    return this;
  }

  public QueryBuilder vsearch(String column, float[] vectorValue) {
    this.orderByAnn = column;
    this.vectorValue = CQLBindValues.getVectorValue(vectorValue);
    return this;
  }

  public static class FunctionCall {
    final String columnName;
    String alias;
    final String functionName;

    boolean isSimilarityFunction;

    private FunctionCall(
        String columnName, String alias, String functionName, boolean isSimilarityFunction) {
      this.columnName = columnName;
      this.alias = alias;
      this.functionName = functionName;
      this.isSimilarityFunction = isSimilarityFunction;
    }

    public static FunctionCall function(String name, String alias, String functionName) {
      return new FunctionCall(name, alias, functionName, false);
    }

    public static FunctionCall similarityFunctionCall(
        String columnName, String similarityFunction) {
      return new FunctionCall(columnName, null, similarityFunction, true);
    }

    public static FunctionCall count(String columnName) {
      return count(columnName, null);
    }

    public static FunctionCall count(String columnName, String alias) {
      return function(columnName, alias, COUNT_FUNCTION_NAME);
    }

    public String getColumnName() {
      return columnName;
    }

    public String getFunctionName() {
      return functionName;
    }

    public String getAlias() {
      return alias;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }
  }
}
