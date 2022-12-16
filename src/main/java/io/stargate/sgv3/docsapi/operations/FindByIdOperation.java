package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.cql.builder.BuiltCondition;
import io.stargate.sgv2.api.common.cql.builder.Predicate;
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
public class FindByIdOperation extends ReadOperation {

  private final String docID;

  public FindByIdOperation(CommandContext commandContext, String docID) {
    super(commandContext);
    this.docID = docID;
  }

  @Override
  protected Uni<ReadOperationPage> executeInternal(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = selectBuilder(getCommandContext());
    return queryExecutor.readDocument(query, null);
  }

  private QueryOuterClass.Query selectBuilder(CommandContext commandContext) {
    return new QueryBuilder()
        .select()
        .column("key", "tx_id", "doc_field_order", "doc_atomic_fields")
        .from(commandContext.database, commandContext.collection)
        .where(BuiltCondition.of("key", Predicate.EQ, Values.of(docID)))
        .build();
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
