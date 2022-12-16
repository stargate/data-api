package io.stargate.sgv3.docsapi.operations;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv3.docsapi.bridge.query.QueryExecutor;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.shredding.ReadableShreddedDocument;
import io.stargate.sgv3.docsapi.shredding.WritableShreddedDocument;
import java.util.ArrayList;
import java.util.List;
import org.javatuples.Pair;

/**
 * Inserts one or more documents with a conditional check to make sure the {@link
 * ReadableShreddedDocument#txID} has not changed if the document has one.
 *
 * <p>If the document does not have one then a normal insert is run.
 *
 * <p>NOTE: we may find we want to collapse this as {@link InsertOperation} into one class later,
 * for now keep separate and there is a bunch of re-use from the super class. This should make it
 * easier to develop the conditional update
 */
public class ConditionalInsertOperation extends ModifyOperation {

  private List<WritableShreddedDocument> docs;

  public ConditionalInsertOperation(CommandContext commandContext, WritableShreddedDocument doc) {
    super(commandContext);
    this.docs = List.of(doc);
  }

  public ConditionalInsertOperation(
      CommandContext commandContext, List<WritableShreddedDocument> docs) {
    super(commandContext);
    // defensive copy.
    this.docs = List.copyOf(docs);
  }

  @Override
  protected Uni<ModifyOperationPage> executeInternal(QueryExecutor queryExecutor) {

    // See notes in InsertOperation for how we could handle large updates, error handled etc etc.

    // TODO at production level we would make multiple async calls and then determine which
    // statements
    // were applied, i.e. the tx_id has not changed. How we do this depends on gRPC

    final Uni<List<Pair<WritableShreddedDocument, Boolean>>> results =
        Multi.createBy()
            .concatenating()
            .streams(Multi.createFrom().items(docs.stream()))
            .onItem()
            .transformToUniAndConcatenate(
                doc -> {
                  if (doc.txID == null) {
                    QueryOuterClass.Query executableInsert = buildInsertQuery(getCommandContext());
                    final Uni<String> s = insertDocument(queryExecutor, executableInsert, doc);
                    final Uni<Pair<WritableShreddedDocument, Boolean>> transform =
                        s.onItem().transform(a -> new Pair(doc, false));
                    return transform;
                  } else {
                    QueryOuterClass.Query executableUpdate = buildUpdateQuery(getCommandContext());
                    final Uni<String> s = updateDocument(queryExecutor, executableUpdate, doc);
                    final Uni<Pair<WritableShreddedDocument, Boolean>> transform =
                        s.onItem().transform(a -> new Pair(doc, true));
                    return transform;
                  }
                })
            .collect()
            .asList();

    return results
        .onItem()
        .transform(
            output -> {
              List<String> insertedIds = new ArrayList<>();
              List<String> updatedIds = new ArrayList<>();
              List<WritableShreddedDocument> insertedDocs = new ArrayList<>();
              List<WritableShreddedDocument> updatedDocs = new ArrayList<>();
              output.stream()
                  .forEach(
                      pair -> {
                        if (pair.getValue1()) {
                          updatedIds.add(pair.getValue0().id);
                          updatedDocs.add(pair.getValue0());
                        } else {
                          insertedDocs.add(pair.getValue0());
                          insertedIds.add(pair.getValue0().id);
                        }
                      });
              return ModifyOperationPage.from(insertedIds, insertedDocs, updatedIds, updatedDocs);
            });
  }

  private static Uni<String> insertDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, WritableShreddedDocument doc) {
    query = bindInsertValues(query, doc);
    return queryExecutor.writeDocument(query).onItem().transform(result -> doc.id);
  }

  private static Uni<String> updateDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, WritableShreddedDocument doc) {
    query = bindUpdateValues(query, doc);
    return queryExecutor
        .writeDocument(query)
        .onItem()
        .transform(
            result -> {
              final QueryOuterClass.Row row = result.getRows(0);
              if (row == null) {
                throw new RuntimeException("Expected one row from conditional execution");
              }
              return doc.id;
            });
  }

  @Override
  public OperationPlan getPlan() {
    return new OperationPlan(true);
  }
}
