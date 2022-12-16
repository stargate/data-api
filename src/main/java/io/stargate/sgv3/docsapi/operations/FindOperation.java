package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.QueryBuilder;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;

/**
 * Get a single document by ID, simple hot code path for well used query path
 *
 * <p>The CommandResolver decides if this is the Operation to run by looking at the filter clause.
 *
 * <p>Operations should only know about the DB and our shredding models.
 */
public class FindOperation extends ReadOperation {
  private final String pagingState;
  private final int limit;

  public FindOperation(CommandContext commandContext, String pagingState, int limit) {
    super(commandContext);
    this.pagingState = pagingState;
    this.limit = limit;
  }

  @Override
  protected Uni<ReadOperationPage> executeInternal(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = selectBuilder(getCommandContext(), limit);
    return queryExecutor.readDocument(query, pagingState);
  }

  private QueryOuterClass.Query selectBuilder(CommandContext commandContext, int limit) {
    final QueryBuilder.QueryBuilder__21 from =
        new QueryBuilder()
            .select()
            .column("key", "tx_id", "doc_field_order", "doc_atomic_fields")
            .from(commandContext.database, commandContext.collection);
    if (limit != 0) {
      return from.limit(limit).build();
    } else {
      return from.build();
    }
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
