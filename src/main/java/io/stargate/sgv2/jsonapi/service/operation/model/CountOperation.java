package io.stargate.sgv2.jsonapi.service.operation.model;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.CountOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.DBFilterBase;
import io.stargate.sgv2.jsonapi.service.operation.model.impl.ReadDocument;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operation that returns count of documents based on the filter condition. Written with the
 * assumption that all variables to be indexed.
 */
public record CountOperation(CommandContext commandContext, List<DBFilterBase> filters)
    implements ReadOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = buildSelectQuery();
    return countDocuments(queryExecutor, query)
        .onItem()
        .transform(docs -> new CountOperationPage(docs.count()));
  }

  private QueryOuterClass.Query buildSelectQuery() {
    List<BuiltCondition> conditions = new ArrayList<>(filters.size());
    for (DBFilterBase filter : filters) {
      conditions.add(filter.get());
    }
    return new QueryBuilder()
        .select()
        .count("key")
        .as("count")
        .from(commandContext.namespace(), commandContext.collection())
        .where(conditions)
        .build();
  }

  @Override
  public Uni<FindResponse> getDocuments(QueryExecutor queryExecutor) {
    return Uni.createFrom().failure(new JsonApiException(ErrorCode.UNSUPPORTED_OPERATION));
  }

  @Override
  public ReadDocument getEmptyDocuments() {
    throw new JsonApiException(ErrorCode.UNSUPPORTED_OPERATION);
  }
}
