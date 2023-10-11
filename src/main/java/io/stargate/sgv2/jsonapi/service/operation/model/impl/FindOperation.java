package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.Expression.And;
import io.stargate.sgv2.api.common.cql.Expression.Expression;
import io.stargate.sgv2.api.common.cql.Expression.Or;
import io.stargate.sgv2.api.common.cql.Expression.Variable;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.update.SetOperation;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ChainedComparator;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindOperation(
    CommandContext commandContext,
    LogicalExpression logicalExpression,
    /**
     * Projection used on document to return; if no changes desired, identity projection. Defined
     * for "pure" read operations: for updates (like {@code findOneAndUpdate}) is passed differently
     * to avoid projection from getting applied before updates.
     */
    DocumentProjector projection,
    String pagingState,
    int limit,
    int pageSize,
    ReadType readType,
    ObjectMapper objectMapper,
    List<OrderBy> orderBy,
    int skip,
    int maxSortReadLimit,
    boolean singleResponse,
    float[] vector)
    implements ReadOperation {

  /**
   * Constructs find operation for unsorted single document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a single document unsorted find
   */
  public static FindOperation unsortedSingle(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      ReadType readType,
      ObjectMapper objectMapper) {

    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        null,
        1,
        1,
        readType,
        objectMapper,
        null,
        0,
        0,
        true,
        null);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pagingState paging state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation unsorted(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pagingState,
      int limit,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pagingState,
        limit,
        pageSize,
        readType,
        objectMapper,
        null,
        0,
        0,
        false,
        null);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation vsearchSingle(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      ReadType readType,
      ObjectMapper objectMapper,
      float[] vector) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        null,
        1,
        1,
        readType,
        objectMapper,
        null,
        0,
        0,
        true,
        vector);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pagingState paging state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation vsearch(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pagingState,
      int limit,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper,
      float[] vector) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pagingState,
        limit,
        pageSize,
        readType,
        objectMapper,
        null,
        0,
        0,
        false,
        vector);
  }

  /**
   * Constructs find operation for sorted single document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pageSize page size for in memory sorting
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param orderBy order by clause
   * @param skip number of elements to skip
   * @param maxSortReadLimit sorting limit
   * @return FindOperation for a single document sorted find
   */
  public static FindOperation sortedSingle(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        null,
        1,
        pageSize,
        readType,
        objectMapper,
        orderBy,
        skip,
        maxSortReadLimit,
        true,
        null);
  }

  /**
   * Constructs find operation for sorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pagingState paging state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size for in memory sorting
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param orderBy order by clause
   * @param skip number of elements to skip
   * @param maxSortReadLimit sorting limit
   * @return FindOperation for a multi document sorted find
   */
  public static FindOperation sorted(
      CommandContext commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pagingState,
      int limit,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pagingState,
        limit,
        pageSize,
        readType,
        objectMapper,
        orderBy,
        skip,
        maxSortReadLimit,
        false,
        null);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().isVectorEnabled();
    if (vector() != null && !vectorEnabled) {
      return Uni.createFrom()
          .failure(
              new JsonApiException(
                  ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED,
                  ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED.getMessage()
                      + commandContext().collection()));
    }
    // get FindResponse
    return getDocuments(queryExecutor, pagingState(), null)

        // map the response to result
        .map(docs -> new ReadOperationPage(docs.docs(), docs.pagingState(), singleResponse));
  }

  /**
   * A operation method which can return FindResponse instead of CommandResult. This method will be
   * used by other commands which needs a document to be read.
   *
   * @param queryExecutor
   * @param pagingState
   * @param additionalIdFilter Used if a additional id filter need to be added to already available
   *     filters
   * @return
   */
  public Uni<FindResponse> getDocuments(
      QueryExecutor queryExecutor, String pagingState, DBFilterBase.IDFilter additionalIdFilter) {

    // ensure we pass failure down if read type is not DOCUMENT or KEY
    // COUNT is not supported
    switch (readType) {
      case SORTED_DOCUMENT -> {
        List<QueryOuterClass.Query> queries = buildSortedSelectQueries(additionalIdFilter);
        return findOrderDocument(
            queryExecutor,
            queries,
            pageSize,
            objectMapper(),
            new ChainedComparator(orderBy(), objectMapper()),
            orderBy().size(),
            skip(),
            limit(),
            maxSortReadLimit(),
            projection());
      }
      case DOCUMENT, KEY -> {
        List<QueryOuterClass.Query> queries = buildSelectQueries(additionalIdFilter);
        return findDocument(
            queryExecutor,
            queries,
            pagingState,
            pageSize,
            ReadType.DOCUMENT == readType,
            objectMapper,
            projection,
            limit());
      }
      default -> {
        JsonApiException failure =
            new JsonApiException(
                ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported find operation read type " + readType);
        return Uni.createFrom().failure(failure);
      }
    }
  }

  /**
   * A operation method which can return ReadDocument with an empty document, if the filter
   * condition has _id filter it will return document with this field added
   *
   * @return
   */
  public ReadDocument getNewDocument() {
    ObjectNode rootNode = objectMapper().createObjectNode();
    DocumentId documentId = null;
    Stack<LogicalExpression> stack = new Stack<>();
    stack.push(logicalExpression);
    while (!stack.empty()) {
      LogicalExpression currentLogicalExpression = stack.pop();
      for (ComparisonExpression currentComparisonExpression :
          currentLogicalExpression.comparisonExpressions) {
        for (DBFilterBase filter : currentComparisonExpression.getDbFilters()) {
          if (filter instanceof DBFilterBase.IDFilter idFilter && idFilter.canAddField()) {
            documentId = idFilter.values.get(0);
            rootNode.putIfAbsent(filter.getPath(), filter.asJson(objectMapper().getNodeFactory()));
          } else {
            if (filter.canAddField()) {
              JsonNode value = filter.asJson(objectMapper().getNodeFactory());
              if (value != null) {
                String filterPath = filter.getPath();
                SetOperation.constructSet(filterPath, value).updateDocument(rootNode);
              }
            }
          }
        }
      }
      for (LogicalExpression subLogicalExpression : currentLogicalExpression.logicalExpressions) {
        stack.push(subLogicalExpression);
      }
    }
    return ReadDocument.from(documentId, null, rootNode);
  }

  /**
   * Builds select query based on filters and additionalIdFilter overrides.
   *
   * @param additionalIdFilter
   * @return Returns a list of queries, where a query is built using element returned by the
   *     buildConditions method.
   */
  private List<QueryOuterClass.Query> buildSelectQueries(DBFilterBase.IDFilter additionalIdFilter) {
    List<Expression<BuiltCondition>> expressions =
        buildExpressions(logicalExpression, additionalIdFilter);
    if (expressions == null) { // in filter, but with empty values, find nothing
      return List.of();
    }
    List<QueryOuterClass.Query> queries = new ArrayList<>(expressions.size());
    expressions.forEach(
        expression -> {
          if (vector() == null) {
            queries.add(
                new QueryBuilder()
                    .select()
                    .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
                    .from(commandContext.namespace(), commandContext.collection())
                    .where(expression)
                    .limit(limit)
                    .build());
          } else {
            QueryOuterClass.Query builtQuery = getVectorSearchQueryByExpression(expression);
            final List<QueryOuterClass.Value> valuesList =
                builtQuery.getValuesOrBuilder().getValuesList();
            final QueryOuterClass.Values.Builder builder = QueryOuterClass.Values.newBuilder();
            valuesList.forEach(builder::addValues);
            builder.addValues(CustomValueSerializers.getVectorValue(vector()));
            queries.add(QueryOuterClass.Query.newBuilder(builtQuery).setValues(builder).build());
          }
        });

    return queries;
  }

  /**
   * A separate method to build vector search query by using expression, expression can contain
   * logic operations like 'or','and'..
   */
  private QueryOuterClass.Query getVectorSearchQueryByExpression(
      Expression<BuiltCondition> expression) {
    QueryOuterClass.Query builtQuery = null;
    if (projection().doIncludeSimilarityScore()) {
      switch (commandContext().similarityFunction()) {
        case COSINE, UNDEFINED -> {
          return new QueryBuilder()
              .select()
              .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
              .similarityCosine(
                  DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME,
                  CustomValueSerializers.getVectorValue(vector()))
              .from(commandContext.namespace(), commandContext.collection())
              .where(expression)
              .limit(limit)
              .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)
              .build();
        }
        case EUCLIDEAN -> {
          return new QueryBuilder()
              .select()
              .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
              .similarityEuclidean(
                  DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME,
                  CustomValueSerializers.getVectorValue(vector()))
              .from(commandContext.namespace(), commandContext.collection())
              .where(expression)
              .limit(limit)
              .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)
              .build();
        }
        case DOT_PRODUCT -> {
          return new QueryBuilder()
              .select()
              .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
              .similarityDotProduct(
                  DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME,
                  CustomValueSerializers.getVectorValue(vector()))
              .from(commandContext.namespace(), commandContext.collection())
              .where(expression)
              .limit(limit)
              .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)
              .build();
        }
        default -> {
          throw new JsonApiException(
              ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME,
              ErrorCode.VECTOR_SEARCH_INVALID_FUNCTION_NAME.getMessage()
                  + commandContext().similarityFunction());
        }
      }
    } else {
      return new QueryBuilder()
          .select()
          .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
          .from(commandContext.namespace(), commandContext.collection())
          .where(expression)
          .limit(limit)
          .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)
          .build();
    }
  }

  /**
   * Builds select query based on filters, sort fields and additionalIdFilter overrides.
   *
   * @param additionalIdFilter
   * @return Returns a list of queries, where a query is built using element returned by the
   *     buildConditions method.
   */
  private List<QueryOuterClass.Query> buildSortedSelectQueries(
      DBFilterBase.IDFilter additionalIdFilter) {
    List<Expression<BuiltCondition>> expressions =
        buildExpressions(logicalExpression, additionalIdFilter);
    if (expressions == null) {
      return List.of();
    }
    String[] columns = sortedDataColumns;
    if (orderBy() != null) {
      List<String> sortColumns = Lists.newArrayList(columns);
      orderBy().forEach(order -> sortColumns.addAll(order.getOrderingColumns()));
      columns = new String[sortColumns.size()];
      sortColumns.toArray(columns);
    }
    final String[] columnsToAdd = columns;
    List<QueryOuterClass.Query> queries = new ArrayList<>(expressions.size());
    expressions.forEach(
        expression ->
            queries.add(
                new QueryBuilder()
                    .select()
                    .column(columnsToAdd)
                    .from(commandContext.namespace(), commandContext.collection())
                    .where(expression)
                    .limit(maxSortReadLimit())
                    .build()));
    return queries;
  }

  public static List<Expression<BuiltCondition>> buildExpressions(
      LogicalExpression logicalExpression, DBFilterBase.IDFilter additionalIdFilter) {
    // after validate in FilterClauseDeserializer, partition key column key will not be nested under
    // OR operator
    // so we can collect all id_conditions, then do a combination to generate separate queries
    List<DBFilterBase.IDFilter> idFilters = new ArrayList<>();
    // expressionWithoutId must be a And(if not null), since we have outer implicit and in the
    // filter
    Expression<BuiltCondition> expressionWithoutId =
        buildExpressionRecursive(logicalExpression, additionalIdFilter, idFilters);

    List<Expression<BuiltCondition>> expressions =
        buildExpressionWithId(additionalIdFilter, expressionWithoutId, idFilters);

    return expressions;
  }

  private static List<Expression<BuiltCondition>> buildExpressionWithId(
      DBFilterBase.IDFilter additionalIdFilter,
      Expression<BuiltCondition> expressionWithoutId,
      List<DBFilterBase.IDFilter> idFilters) {
    List<Expression<BuiltCondition>> expressionsWithId = new ArrayList<>();

    if (idFilters.size() > 1) {
      throw new JsonApiException(
          ErrorCode.FILTER_MULTIPLE_ID_FILTER, ErrorCode.FILTER_MULTIPLE_ID_FILTER.getMessage());
    }
    if (idFilters.isEmpty()
        && additionalIdFilter == null) { // no idFilters in filter clause and no additionalIdFilter
      if (expressionWithoutId == null) {
        expressionsWithId.add(null); // should find everything
        return expressionsWithId;
      } else {
        return new ArrayList<>(List.of(expressionWithoutId)); // mutable for later sorting
      }
    }

    // have an idFilter
    DBFilterBase.IDFilter idFilter =
        additionalIdFilter != null ? additionalIdFilter : idFilters.get(0);
    if (idFilter.operator == DBFilterBase.IDFilter.Operator.IN && idFilter.getAll().isEmpty()) {
      return null; // should find nothing
    }
    // idFilter's operator is IN or EQ, for both, we can follow the split query logic
    List<BuiltCondition> inSplit =
        idFilters.isEmpty() ? new ArrayList<>() : idFilters.get(0).getAll();
    if (additionalIdFilter != null) {
      inSplit = additionalIdFilter.getAll(); // override
    }
    // split n queries by id
    return inSplit.stream()
        .map(
            idCondition -> {
              Expression<BuiltCondition> newExpression =
                  expressionWithoutId == null
                      ? Variable.of(idCondition)
                      : And.of(Variable.of(idCondition), expressionWithoutId);
              return newExpression;
            })
        .collect(Collectors.toList());
  }

  private static Expression<BuiltCondition> buildExpressionRecursive(
      LogicalExpression logicalExpression,
      DBFilterBase.IDFilter additionalIdFilter,
      List<DBFilterBase.IDFilter> idConditionExpressions) {
    List<Expression<BuiltCondition>> conditionExpressions = new ArrayList<>();
    for (LogicalExpression subLogicalExpression : logicalExpression.logicalExpressions) {
      final Expression<BuiltCondition> subExpressionCondition =
          buildExpressionRecursive(
              subLogicalExpression, additionalIdFilter, idConditionExpressions);
      if (subExpressionCondition == null) {
        continue;
      }
      conditionExpressions.add(subExpressionCondition);
    }
    for (ComparisonExpression comparisonExpression : logicalExpression.comparisonExpressions) {
      for (DBFilterBase dbFilter : comparisonExpression.getDbFilters()) {
        if (dbFilter instanceof DBFilterBase.InFilter inFilter) {
          List<BuiltCondition> inFilterConditions = inFilter.getAll();
          if (!inFilterConditions.isEmpty()) {
            List<Variable<BuiltCondition>> inConditions =
                inFilterConditions.stream().map(Variable::of).toList();
            conditionExpressions.add(Or.of(inConditions));
          }
        } else if (dbFilter instanceof DBFilterBase.IDFilter idFilter) {
          if (additionalIdFilter != null) continue;
          idConditionExpressions.add(idFilter);
        } else {
          conditionExpressions.add(Variable.of(dbFilter.get()));
        }
      }
    }
    // current logicalExpression is empty (implies sub-logicalExpression and
    // sub-comparisonExpression are all empty)
    if (conditionExpressions.isEmpty()) {
      return null;
    }
    return logicalExpression.getLogicalRelation().equals("and")
        ? And.of(conditionExpressions)
        : Or.of(conditionExpressions);
  }

  /**
   * Represents sort field name and option to be sorted ascending/descending.
   *
   * @param column
   * @param ascending
   */
  public record OrderBy(String column, boolean ascending) {
    /**
     * Returns index column name with field name as entry key like query_text_values['username']
     *
     * @return
     */
    public List<String> getOrderingColumns() {
      return sortIndexColumns.stream()
          .map(col -> col.formatted(column()))
          .collect(Collectors.toList());
    }
  }
}
