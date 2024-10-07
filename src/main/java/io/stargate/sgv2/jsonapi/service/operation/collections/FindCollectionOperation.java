package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.bpodgursky.jbool_expressions.Expression;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCodeV1;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.cql.builder.Query;
import io.stargate.sgv2.jsonapi.service.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.ReadOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.builder.BuiltCondition;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.CollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.filters.collection.IDCollectionFilter;
import io.stargate.sgv2.jsonapi.service.operation.query.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.query.DBLogicalExpression;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;
import io.stargate.sgv2.jsonapi.service.shredding.collections.DocumentId;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Operation that returns the documents or its key based on the filter condition. */
public record FindCollectionOperation(
    CommandContext<CollectionSchemaObject> commandContext,
    DBLogicalExpression dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param includeSortVector include sort vector in the response
   * @return FindCollectionOperation for a single document unsorted find
   */
  public static FindCollectionOperation unsortedSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
      DocumentProjector projection,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      boolean includeSortVector) {

    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use * @param includeSortVector include sort vector in the
   *     response
   * @return FindCollectionOperation for a multi document unsorted find
   */
  public static FindCollectionOperation unsorted(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
      DocumentProjector projection,
      String pageState,
      int limit,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      boolean includeSortVector) {
    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param vector vector to search
   * @param includeSortVector include sort vector in the response
   * @return FindCollectionOperation for a multi document unsorted find
   */
  public static FindCollectionOperation vsearchSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
      DocumentProjector projection,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      float[] vector,
      boolean includeSortVector) {
    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size
   * @param readType type of the read
   * @param objectMapper object mapper to use * @param vector vector to search * @param
   * @param includeSortVector include sort vector in the response
   * @return FindCollectionOperation for a multi document unsorted find
   */
  public static FindCollectionOperation vsearch(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
      DocumentProjector projection,
      String pageState,
      int limit,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      float[] vector,
      boolean includeSortVector) {
    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param pageSize page size for in memory sorting
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param orderBy order by clause
   * @param skip number of elements to skip
   * @param maxSortReadLimit sorting limit * @param includeSortVector include sort vector in the
   *     response
   * @return FindCollectionOperation for a single document sorted find
   */
  public static FindCollectionOperation sortedSingle(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
      DocumentProjector projection,
      int pageSize,
      CollectionReadType readType,
      ObjectMapper objectMapper,
      List<OrderBy> orderBy,
      int skip,
      int maxSortReadLimit,
      boolean includeSortVector) {
    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
   * @param dbLogicalExpression expression contains filters and their logical relation
   * @param projection projections, see FindCollectionOperation#projection
   * @param pageState page state to use
   * @param limit limit of rows to fetch
   * @param pageSize page size for in memory sorting
   * @param readType type of the read
   * @param objectMapper object mapper to use
   * @param orderBy order by clause
   * @param skip number of elements to skip
   * @param maxSortReadLimit sorting limit
   * @param includeSortVector include sort vector in the response
   * @return FindCollectionOperation for a multi document sorted find
   */
  public static FindCollectionOperation sorted(
      CommandContext<CollectionSchemaObject> commandContext,
      DBLogicalExpression dbLogicalExpression,
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
    return new FindCollectionOperation(
        commandContext,
        dbLogicalExpression,
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
              ErrorCodeV1.VECTOR_SEARCH_NOT_SUPPORTED.toApiException(
                  "%s", commandContext().schemaObject().name().table()));
    }
    // get FindResponse
    return getDocuments(dataApiRequestInfo, queryExecutor, pageState(), null)

        // map the response to result
        .map(
            docs -> {
              // TODO: why is this here and not higher up where it can happen for any command result
              // ?
              commandContext
                  .jsonProcessingMetricsReporter()
                  .reportJsonReadDocsMetrics(commandContext().commandName(), docs.docs().size());
              return new ReadOperationPage(
                  docs.docs(), singleResponse, docs.pageState(), includeSortVector(), vector());
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
            ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
                "Unsupported find operation read type `%s`", readType);
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
    final var stack = new Stack<DBLogicalExpression>();
    stack.push(dbLogicalExpression);

    while (!stack.empty()) {
      var currentDbLogicalExpression = stack.pop();

      for (DBFilterBase filter : dbLogicalExpression.filters()) {
        // every filter must be a collection filter, because we are making a new document and we
        // only do this for docs
        if (filter instanceof IDCollectionFilter) {
          IDCollectionFilter idFilter = (IDCollectionFilter) filter;
          documentId = idFilter.getSingularDocumentId();
          idFilter
              .updateForNewDocument(objectMapper().getNodeFactory())
              .ifPresent(setOperation -> setOperation.updateDocument(rootNode));
        } else if (filter instanceof CollectionFilter) {
          CollectionFilter collectionFilter = (CollectionFilter) filter;
          collectionFilter
              .updateForNewDocument(objectMapper().getNodeFactory())
              .ifPresent(setOperation -> setOperation.updateDocument(rootNode));
        } else {
          throw ErrorCodeV1.SERVER_INTERNAL_ERROR.toApiException(
              "Unsupported filter type in getNewDocument: %s", filter.getClass().getName());
        }
      }

      currentDbLogicalExpression.subExpressions().forEach(stack::push);
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
        ExpressionBuilder.buildExpressions(dbLogicalExpression, additionalIdFilter);
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
                        commandContext.schemaObject().name().keyspace(),
                        commandContext.schemaObject().name().table())
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
              commandContext.schemaObject().name().keyspace(),
              commandContext.schemaObject().name().table())
          .where(expression)
          .limit(limit)
          .vsearch(DocumentConstants.Fields.VECTOR_SEARCH_INDEX_COLUMN_NAME, vector())
          .build();
    } else {
      return new QueryBuilder()
          .select()
          .column(CollectionReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
          .from(
              commandContext.schemaObject().name().keyspace(),
              commandContext.schemaObject().name().table())
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
        ExpressionBuilder.buildExpressions(dbLogicalExpression, additionalIdFilter);
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
                      commandContext.schemaObject().name().keyspace(),
                      commandContext.schemaObject().name().table())
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
