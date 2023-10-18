package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.bpodgursky.jbool_expressions.And;
import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Or;
import com.bpodgursky.jbool_expressions.Variable;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindOperation(
    CommandContext commandContext,
    List<DBFilterBase> filters,
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
   * @param filters filters to match a document
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a single document unsorted find
   */
  public static FindOperation unsortedSingle(
      CommandContext commandContext,
      List<DBFilterBase> filters,
      DocumentProjector projection,
      ReadType readType,
      ObjectMapper objectMapper) {

    return new FindOperation(
        commandContext,
        filters,
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
   * @param filters filters to match a document
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
      List<DBFilterBase> filters,
      DocumentProjector projection,
      String pagingState,
      int limit,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper) {
    return new FindOperation(
        commandContext,
        filters,
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
   * @param filters filters to match a document
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation vsearchSingle(
      CommandContext commandContext,
      List<DBFilterBase> filters,
      DocumentProjector projection,
      ReadType readType,
      ObjectMapper objectMapper,
      float[] vector) {
    return new FindOperation(
        commandContext,
        filters,
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
   * @param filters filters to match a document
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
      List<DBFilterBase> filters,
      DocumentProjector projection,
      String pagingState,
      int limit,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper,
      float[] vector) {
    return new FindOperation(
        commandContext,
        filters,
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
   * @param filters filters to match a document
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
      List<DBFilterBase> filters,
      DocumentProjector projection,
      int pageSize,
      ReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit) {
    return new FindOperation(
        commandContext,
        filters,
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
   * @param filters filters to match a document
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
      List<DBFilterBase> filters,
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
        filters,
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

    if (Boolean.getBoolean("MOCK_BRIDGE") || System.getenv("MOCK_BRIDGE") != null) {

      DBFilterBase.IDFilter filter = (DBFilterBase.IDFilter) filters.get(0);
      DocumentId id = filter.values.get(0);
      String tableName = "keyvalue";
      var cql =
          String.format(
              "select key, tx_id, doc_json from %s where key = (1, '%s');",
              tableName, id.asDBKey());

      var rows = CQLSessionHandler.getSession().execute(cql);
      var row = rows.one();

      try {
        var readDocument =
            ReadDocument.from(
                DocumentId.fromString(id.asDBKey()),
                row.getUuid("tx_id"),
                objectMapper.readTree(row.getString("doc_json")));

        return Uni.createFrom().item(new FindResponse(List.of(readDocument), null));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

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
    for (DBFilterBase filter : filters) {
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
    // if the query has "$in" operator for non-id field, buildCondition should return List of
    // Expression
    // that is the reason having this boolean
    // TODO queryBuilder change for where(Expression<BuildCondition) to handle null Expression
    // TODO then we can fully rely on List<Expression<BuildCondition>> instead of
    // TODO List<List<BuildCondition>>
    final Optional<DBFilterBase> inFilter =
        filters.stream().filter(filter -> filter instanceof DBFilterBase.InFilter).findFirst();
    if (inFilter.isPresent()) {
      // This if block handles filter with "$in" for non-id field
      List<Expression<BuiltCondition>> expressions = buildConditionExpressions(additionalIdFilter);
      if (expressions == null) {
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
    } else {
      // This if block handles filter with no "$in" for non-id field
      List<List<BuiltCondition>> conditions = buildConditions(additionalIdFilter);
      if (conditions == null) {
        return List.of();
      }
      List<QueryOuterClass.Query> queries = new ArrayList<>(conditions.size());
      conditions.forEach(
          condition -> {
            if (vector() == null) {
              queries.add(
                  new QueryBuilder()
                      .select()
                      .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
                      .from(commandContext.namespace(), commandContext.collection())
                      .where(condition)
                      .limit(limit)
                      .build());
            } else {
              QueryOuterClass.Query builtQuery = getVectorSearchQuery(condition);
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
  }

  /** Making it a separate method to build vector search query as there are many options */
  private QueryOuterClass.Query getVectorSearchQuery(List<BuiltCondition> conditions) {
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
              .where(conditions)
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
              .where(conditions)
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
              .where(conditions)
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
          .where(conditions)
          .limit(limit)
          .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME)
          .build();
    }
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
    // if the query has "$in" operator for non-id field, buildCondition should return List of
    // Expression
    // that is the reason having this boolean
    // TODO queryBuilder change for where(Expression<BuildCondition) to handle null Expression
    // TODO then we can fully rely on List<Expression<BuildCondition>> instead of
    // TODO List<List<BuildCondition>>

    final Optional<DBFilterBase> inFilter =
        filters.stream().filter(filter -> filter instanceof DBFilterBase.InFilter).findFirst();
    if (inFilter.isPresent()) {
      // This if block handles filter with "$in" for non-id field
      List<Expression<BuiltCondition>> expressions = buildConditionExpressions(additionalIdFilter);
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

    } else {
      // This if block handles filter with "$in" for non-id field
      List<List<BuiltCondition>> conditions = buildConditions(additionalIdFilter);
      if (conditions == null) {
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
      List<QueryOuterClass.Query> queries = new ArrayList<>(conditions.size());
      conditions.forEach(
          condition ->
              queries.add(
                  new QueryBuilder()
                      .select()
                      .column(columnsToAdd)
                      .from(commandContext.namespace(), commandContext.collection())
                      .where(condition)
                      .limit(maxSortReadLimit())
                      .build()));
      return queries;
    }
  }

  /**
   * Builds select query based on filters and additionalIdFilter overrides.
   *
   * @param additionalIdFilter Used if a additional id filter need to be added to already available
   *     contions
   * @return Returns a list of list, where outer list represents conditions to be sent as multiple
   *     queries. Only in condition on _id is returns multiple queries.
   */
  private List<List<BuiltCondition>> buildConditions(DBFilterBase.IDFilter additionalIdFilter) {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    DBFilterBase.IDFilter idFilterToUse = additionalIdFilter;
    // if we have id filter overwrite ignore existing IDFilter
    boolean idFilterOverwrite = additionalIdFilter != null;
    for (DBFilterBase filter : filters) {
      if (!(filter instanceof DBFilterBase.IDFilter idFilter)) {
        conditions.add(filter.get());
      } else {
        if (!idFilterOverwrite) {
          idFilterToUse = idFilter;
        }
      }
    }
    // then add id filter if available, in case of multiple `in` conditions we need to send multiple
    // condtions
    if (idFilterToUse != null) {
      final List<BuiltCondition> inSplit = idFilterToUse.getAll();
      if (inSplit.isEmpty()) {
        // returning null means return empty result
        return null;
      } else {
        return inSplit.stream()
            .map(
                idCondition -> {
                  List<BuiltCondition> conditionsWithId = new ArrayList<>(conditions);
                  conditionsWithId.add(idCondition);
                  return conditionsWithId;
                })
            .collect(Collectors.toList());
      }
    } else {
      return List.of(conditions);
    }
  }

  /**
   * Builds select query based on filters and additionalIdFilter overrides. return expression to
   * pass logic operation information, eg 'or'
   */
  private List<Expression<BuiltCondition>> buildConditionExpressions(
      DBFilterBase.IDFilter additionalIdFilter) {
    Expression<BuiltCondition> conditionExpression = null;
    //        for (DBFilterBase filter : filters) {
    //            // all filters will be in And.of
    //            // if the filter is DBFilterBase.INFilter
    //            // inside of this filter, it has getAll method to return a list of BuiltCondition,
    // these
    //            // BuiltCondition will be in Or.of
    //        }
    DBFilterBase.IDFilter idFilterToUse = additionalIdFilter;
    // if we have id filter overwrite ignore existing IDFilter
    boolean idFilterOverwrite = additionalIdFilter != null;
    for (DBFilterBase filter : filters) {
      if (filter instanceof DBFilterBase.InFilter inFilter) {
        List<BuiltCondition> conditions = inFilter.getAll();
        if (!conditions.isEmpty()) {
          List<Variable<BuiltCondition>> variableConditions =
              conditions.stream().map(Variable::of).toList();
          conditionExpression =
              conditionExpression == null
                  ? Or.of(variableConditions)
                  : And.of(Or.of(variableConditions), conditionExpression);
        }
      } else if (filter instanceof DBFilterBase.IDFilter idFilter) {
        if (!idFilterOverwrite) {
          idFilterToUse = idFilter;
        }
      } else {
        conditionExpression =
            conditionExpression == null
                ? Variable.of(filter.get())
                : And.of(Variable.of(filter.get()), conditionExpression);
      }
    }

    if (idFilterToUse != null) {
      final List<BuiltCondition> inSplit = idFilterToUse.getAll();
      if (inSplit.isEmpty()) {
        return null;
      } else {
        // split n queries by id
        Expression<BuiltCondition> tempExpression = conditionExpression;
        return inSplit.stream()
            .map(
                idCondition -> {
                  Expression<BuiltCondition> newExpression =
                      tempExpression == null
                          ? Variable.of(idCondition)
                          : And.of(Variable.of((idCondition)), tempExpression);
                  return newExpression;
                })
            .collect(Collectors.toList());
      }
    } else {
      return conditionExpression == null ? null : List.of(conditionExpression); // only one query
    }
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
