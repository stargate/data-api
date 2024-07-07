package io.stargate.sgv2.jsonapi.service.operation.model.collections;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.ComparisonExpression;
import io.stargate.sgv2.jsonapi.api.model.command.clause.filter.LogicalExpression;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cql.builder.Query;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.model.filters.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.filters.collection.CollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.model.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    LogicalExpression logicalExpression,
    /**
     * Projection used on document to return; if no changes desired, identity projection. Defined
     * for "pure" read operations: for updates (like {@code findOneAndUpdate}) is passed differently
     * to avoid projection from getting applied before updates.
     */
    DocumentProjector projection,
    String pageState,
    int limit,
    int pageSize,
    CollectionReadType readType,
    ObjectMapper objectMapper,
    List<OrderBy> orderBy,
    int skip,
    int maxSortReadLimit,
    boolean singleResponse,
    float[] vector,

    /** Whether to include the sort vector in the response. This is used for vector search. */
    boolean includeSortVector)
    implements CollectionReadOperation {

  /**
   * Constructs find operation for unsorted single document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param includeSortVector include sort vector in the response
   * @return FindOperation for a single document unsorted find
   */
  public static FindOperation unsortedSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      boolean includeSortVector) {

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
        null,
        includeSortVector);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use * @param includeSortVector include sort vector in the
   *     response
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation unsorted(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pageState,
      int limit,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      boolean includeSortVector) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pageState,
        limit,
        pageSize,
        readType,
        objectMapper,
        null,
        0,
        0,
        false,
        null,
        includeSortVector);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param vector vector to search
   * @param includeSortVector include sort vector in the response
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation vsearchSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      float[] vector,
      boolean includeSortVector) {
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
        vector,
        includeSortVector);
  }

  /**
   * Constructs find operation for unsorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use * @param vector vector to search * @param
   * @param includeSortVector include sort vector in the response
   * @return FindOperation for a multi document unsorted find
   */
  public static FindOperation vsearch(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pageState,
      int limit,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      float[] vector,
      boolean includeSortVector) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pageState,
        limit,
        pageSize,
        readType,
        objectMapper,
        null,
        0,
        0,
        false,
        vector,
        includeSortVector);
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
   * @param maxSortReadLimit sorting limit * @param includeSortVector include sort vector in the
   *     response
   * @return FindOperation for a single document sorted find
   */
  public static FindOperation sortedSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit,
      boolean includeSortVector) {
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
        null,
        includeSortVector);
  }

  /**
   * Constructs find operation for sorted multi document find.
   *
   * @param commandContext command context
   * @param logicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size for in memory sorting
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param orderBy order by clause
   * @param skip number of elements to skip
   * @param maxSortReadLimit sorting limit
   * @param includeSortVector include sort vector in the response
   * @return FindOperation for a multi document sorted find
   */
  public static FindOperation sorted(
      CommandContext<CollectionSchemaObject> commandContext,
      LogicalExpression logicalExpression,
      DocumentProjector projection,
      String pageState,
      int limit,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit,
      boolean includeSortVector) {
    return new FindOperation(
        commandContext,
        logicalExpression,
        projection,
        pageState,
        limit,
        pageSize,
        readType,
        objectMapper,
        orderBy,
        skip,
        maxSortReadLimit,
        false,
        null,
        includeSortVector);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {
    final boolean vectorEnabled = commandContext().schemaObject().isVectorEnabled();
    if (vector() != null && !vectorEnabled) {
      return Uni.createFrom()
          .failure(
              new JsonApiException(
                  ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED,
                  ErrorCode.VECTOR_SEARCH_NOT_SUPPORTED.getMessage()
                      + commandContext().schemaObject().name.table()));
    }
    // get FindResponse
    return getDocuments(dataApiRequestInfo, queryExecutor, pageState(), null)

        // map the response to result
        .map(
            docs -> {
              commandContext
                  .jsonProcessingMetricsReporter()
                  .reportJsonReadDocsMetrics(commandContext().commandName(), docs.docs().size());
              return new ReadOperationPage(
                  docs.docs(), docs.pageState(), singleResponse, includeSortVector(), vector());
            });
  }

  /**
   * A operation method which can return FindResponse instead of CommandResult. This method will be
   * used by other commands which needs a document to be read.
   *
   * @param queryExecutor
   * @param pageState
   * @param additionalIdFilter Used if a additional id filter need to be added to already available
   *     filters
   * @return
   */
  public Uni<FindResponse> getDocuments(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      String pageState,
      IDCollectionFilter additionalIdFilter) {

    // ensure we pass failure down if read type is not DOCUMENT or KEY
    // COUNT is not supported
    switch (readType) {
      case SORTED_DOCUMENT -> {
        List<SimpleStatement> queries = buildSortedSelectQueries(additionalIdFilter);
        return findOrderDocument(
            dataApiRequestInfo,
            queryExecutor,
            queries,
            pageSize,
            objectMapper(),
            new ChainedComparator(orderBy(), objectMapper()),
            orderBy().size(),
            skip(),
            limit(),
            maxSortReadLimit(),
            projection(),
            vector() != null,
            commandContext.commandName(),
            commandContext.jsonProcessingMetricsReporter());
      }
      case DOCUMENT, KEY -> {
        List<SimpleStatement> queries = buildSelectQueries(additionalIdFilter);
        return findDocument(
            dataApiRequestInfo,
            queryExecutor,
            queries,
            pageState,
            pageSize,
            CollectionReadType.DOCUMENT == readType,
            objectMapper,
            projection,
            limit(),
            vector() != null,
            commandContext.commandName(),
            commandContext.jsonProcessingMetricsReporter());
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

    final var rootNode = objectMapper().createObjectNode();
    DocumentId documentId = null;
    final var stack = new Stack<LogicalExpression>();
    stack.push(logicalExpression);

    while (!stack.empty()) {
      var currentLogicalExpression = stack.pop();

      for (ComparisonExpression currentComparisonExpression :
          currentLogicalExpression.comparisonExpressions) {
        for (DBFilterBase filter : currentComparisonExpression.getDbFilters()) {
          // every filter must be a collection filter, because we are making a new document and we
          // only do this for docs
          // TODO: move ot modern swtich with pattern matching
          if (filter instanceof IDCollectionFilter) {
            IDCollectionFilter idFilter = (IDCollectionFilter) filter;
            documentId = idFilter.getSingularDocumentId();
            idFilter
                .updateForNewDocument(objectMapper().getNodeFactory())
                .ifPresent(setOperation -> setOperation.updateDocument(rootNode));
          } else if (filter instanceof CollectionFilter) {
            CollectionFilter f = (CollectionFilter) filter;
            f.updateForNewDocument(objectMapper().getNodeFactory())
                .ifPresent(setOperation -> setOperation.updateDocument(rootNode));
          } else {
            throw new RuntimeException(
                "Unsupported filter type in getNewDocument: " + filter.getClass().getName());
          }
        }
      }

      currentLogicalExpression.logicalExpressions.forEach(stack::push);
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
  private List<SimpleStatement> buildSelectQueries(IDCollectionFilter additionalIdFilter) {
    final List<Expression<BuiltCondition>> expressions =
        ExpressionBuilder.buildExpressions(logicalExpression, additionalIdFilter);
    if (expressions == null) { // find nothing
      return List.of();
    }
    List<SimpleStatement> queries = new ArrayList<>(expressions.size());
    expressions.forEach(
        expression -> {
          final Query query;
          if (vector() == null) {
            query =
                new QueryBuilder()
                    .select()
                    .column(
                        CollectionReadType.DOCUMENT == readType
                            ? documentColumns
                            : documentKeyColumns)
                    .from(
                        commandContext.schemaObject().name.keyspace(),
                        commandContext.schemaObject().name.table())
                    .where(expression)
                    .limit(limit)
                    .build();
          } else {
            query = getVectorSearchQueryByExpression(expression);
          }
          queries.add(query.queryToStatement());
        });

    return queries;
  }

  /**
   * A separate method to build vector search query by using expression, expression can contain
   * logic operations like 'or','and'..
   */
  private Query getVectorSearchQueryByExpression(Expression<BuiltCondition> expression) {
    if (projection().doIncludeSimilarityScore()) {
      return new QueryBuilder()
          .select()
          .column(CollectionReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
          .similarityFunction(
              DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME,
              commandContext().schemaObject().similarityFunction())
          .from(
              commandContext.schemaObject().name.keyspace(),
              commandContext.schemaObject().name.table())
          .where(expression)
          .limit(limit)
          .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME, vector())
          .build();
    } else {
      return new QueryBuilder()
          .select()
          .column(CollectionReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
          .from(
              commandContext.schemaObject().name.keyspace(),
              commandContext.schemaObject().name.table())
          .where(expression)
          .limit(limit)
          .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME, vector())
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
  private List<SimpleStatement> buildSortedSelectQueries(IDCollectionFilter additionalIdFilter) {
    final List<Expression<BuiltCondition>> expressions =
        ExpressionBuilder.buildExpressions(logicalExpression, additionalIdFilter);
    if (expressions == null) { // find nothing
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
    List<SimpleStatement> queries = new ArrayList<>(expressions.size());
    expressions.forEach(
        expression -> {
          final Query query =
              new QueryBuilder()
                  .select()
                  .column(columnsToAdd)
                  .from(
                      commandContext.schemaObject().name.keyspace(),
                      commandContext.schemaObject().name.table())
                  .where(expression)
                  .limit(maxSortReadLimit())
                  .build();
          queries.add(query.queryToStatement());
        });

    return queries;
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
