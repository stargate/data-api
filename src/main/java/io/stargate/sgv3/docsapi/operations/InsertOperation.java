package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.List;

/**
 * Insert one or more documents.
 *
 * <p>The CommandResolver decides if this is the Operation to run by looking at the filter clause.
 *
 * <p>Operations should only know about the DB and our shredding models.
 */
public class InsertOperation extends ModifyOperation {

  private List<WritableShreddedDocument> docs;

  public InsertOperation(CommandContext commandContext, WritableShreddedDocument doc) {
    super(commandContext);
    this.docs = List.of(doc);
  }

  public InsertOperation(CommandContext commandContext, List<WritableShreddedDocument> docs) {
    super(commandContext);
    // defensive copy.
    this.docs = List.copyOf(docs);
  }

  @Override
  protected Uni<ModifyOperationPage> executeInternal(QueryExecutor queryExecutor) {
    final QueryOuterClass.Query query = buildInsertQuery(getCommandContext());
    final Uni<List<String>> ids =
        Multi.createBy()
            .concatenating()
            .streams(Multi.createFrom().items(docs.stream()))
            .onItem()
            .transformToUniAndConcatenate(doc -> insertDocument(queryExecutor, query, doc))
            .collect()
            .asList();
    return ids.onItem()
        .transform(
            insertedIds -> ModifyOperationPage.from(insertedIds, docs, List.of(), List.of()));
  }

  private static Uni<String> insertDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, WritableShreddedDocument doc) {
    query = bindInsertValues(query, doc);
    return queryExecutor.writeDocument(query).onItem().transform(result -> doc.id);
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
