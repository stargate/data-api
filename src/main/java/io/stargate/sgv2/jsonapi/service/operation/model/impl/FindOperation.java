package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Full dynamic query generation for any of the types of filtering we can do against the the db
 * table.
 *
 * <p>Create with a series of filters that are implicitly AND'd together.
 */
public record FindOperation(
    CommandContext commandContext,
    List<DBFilterBase> filters,
    String pagingState,
    int limit,
    int pageSize,
    ReadType readType,
    ObjectMapper objectMapper)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    return getDocuments(queryExecutor)
        .onItem()
        .transform(docs -> new ReadOperationPage(docs.docs(), docs.count(), docs.pagingState()));
  }

  @Override
  public Uni<FindResponse> getDocuments(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = buildSelectQuery();
    switch (readType) {
      case DOCUMENT -> {
        return findDocument(queryExecutor, query, pagingState, pageSize, true, objectMapper);
      }
      case KEY -> {
        return findDocument(queryExecutor, query, pagingState, pageSize, false, objectMapper);
      }
      case COUNT -> {
        return countDocuments(queryExecutor, query);
      }
    }
    return null;
  }

  private QueryOuterClass.Query buildSelectQuery() {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    for (DBFilterBase filter : filters) {
      conditions.add(filter.get());
    }
    switch (readType) {
      case DOCUMENT:
      case KEY:
        {
          return new QueryBuilder()
              .select()
              .column(ReadType.DOCUMENT == readType ? documentColumns : documentKeyColumns)
              .from(commandContext.namespace(), commandContext.collection())
              .where(conditions)
              .limit(limit)
              .build();
        }
      case COUNT:
        {
          return new QueryBuilder()
              .select()
              .count("key")
              .as("count")
              .from(commandContext.namespace(), commandContext.collection())
              .where(conditions)
              .build();
        }
    }
    return null;
  }

  public enum ReadType {
    DOCUMENT,
    KEY,
    COUNT
  }
}
