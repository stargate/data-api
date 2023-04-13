package io.stargate.sgv2.jsonapi.service.operation.model.impl;

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
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.ChainedComparator;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadType;
import io.stargate.sgv2.jsonapi.service.projection.DocumentProjector;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.ArrayList;
import java.util.List;
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
    int maxSortReadLimit)
    implements ReadOperation {

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
        0);
  }

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
        maxSortReadLimit);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    // get FindResponse
    return getDocuments(queryExecutor, pagingState(), null)

        // map the response to result
        .map(docs -> new ReadOperationPage(docs.docs(), docs.pagingState()));
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
        List<QueryOuterClass.Query> queries = buildSortedSelectQuery(additionalIdFilter);
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
        List<QueryOuterClass.Query> queries = buildSelectQuery(additionalIdFilter);
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
      if (filter instanceof DBFilterBase.IDFilter idFilter) {
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

  // builds select query
  private List<QueryOuterClass.Query> buildSelectQuery(DBFilterBase.IDFilter additionalIdFilter) {
    List<List<BuiltCondition>> conditions = buildConditions(additionalIdFilter);
    List<QueryOuterClass.Query> queries = new ArrayList<>(conditions.size());
    conditions.forEach(
        condition ->
            queries.add(
                new QueryBuilder()
                    .select()
                    .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
                    .from(commandContext.namespace(), commandContext.collection())
                    .where(condition)
                    .limit(limit)
                    .build()));
    return queries;
  }

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
    // then add id overwrite if there
    if (idFilterToUse != null) {
      return idFilterToUse.getAll().stream()
          .map(
              idCondition -> {
                List<BuiltCondition> conditionsWithId = new ArrayList<>(conditions);
                conditionsWithId.add(idCondition);
                return conditionsWithId;
              })
          .collect(Collectors.toList());
    } else {
      return List.of(conditions);
    }
  }

  private List<QueryOuterClass.Query> buildSortedSelectQuery(
      DBFilterBase.IDFilter additionalIdFilter) {
    List<List<BuiltCondition>> conditions = buildConditions(additionalIdFilter);

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
