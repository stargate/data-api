package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;

/**
 * Get a single document by a single text field.
 *
 * <p>Hot code path for a simple query that may get used a lot.
 */
public class FindOneByOneTextOperation extends ReadOperation {

  private final String fieldName;
  private final String textValue;

  public FindOneByOneTextOperation(
      CommandContext commandContext, String fieldName, String textValue) {
    super(commandContext);
    this.fieldName = fieldName;
    this.textValue = textValue;
  }

  @Override
  protected Uni<ReadOperationPage> executeInternal(QueryExecutor queryExecutor) {
    QueryOuterClass.Query query = selectBuilder(getCommandContext());
    return queryExecutor.readDocument(query, null);
  }

  private QueryOuterClass.Query selectBuilder(CommandContext commandContext) {
    String select =
        "SELECT key, tx_id, doc_field_order, doc_atomic_fields FROM %s.%s WHERE query_text_values[?] = ? LIMIT 1";
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(fieldName))
            .addValues(Values.of(textValue));
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(select, commandContext.database, commandContext.collection))
        .setValues(values)
        .build();
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
