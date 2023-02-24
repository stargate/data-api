package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Executes readOperation to get the documents ids based on filter condition. All the ids are
 * deleted as LWT based on the id and tx_id.
 */
public record DeleteOperation(
    CommandContext commandContext, ReadOperation readOperation, int deleteLimit)
    implements ModifyOperation {
  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final Boolean[] moreData = {false};
    final QueryOuterClass.Query delete = buildDeleteQuery();
    final Multi<ReadOperation.FindResponse> findResponses =
        Multi.createBy()
            .repeating()
            .uni(
                () -> new AtomicReference<String>(null),
                stateRef -> {
                  Uni<ReadOperation.FindResponse> docsToDelete =
                      readOperation().getDocuments(queryExecutor, stateRef.get());
                  return docsToDelete
                      .onItem()
                      .invoke(findResponse -> stateRef.set(findResponse.pagingState()));
                })
            .whilst(findResponse -> findResponse.pagingState() != null);
    AtomicInteger totalCount = new AtomicInteger(0);
    final Uni<List<DocumentId>> ids =
        findResponses
            .onItem()
            .transformToMulti(
                findResponse -> {
                  final List<ReadDocument> docs = findResponse.docs();
                  if (totalCount.get() + docs.size() <= deleteLimit) {
                    totalCount.addAndGet(docs.size());
                    return Multi.createFrom().items(docs.stream());
                  } else {
                    int needed = deleteLimit - totalCount.get();
                    moreData[0] = true;
                    return Multi.createFrom()
                        .items(findResponse.docs().subList(0, needed).stream());
                  }
                })
            .concatenate()
            .onItem()
            .transformToUniAndConcatenate(
                readDocument -> deleteDocument(queryExecutor, delete, readDocument))
            .collect()
            .asList();

    /*final Uni<List<DocumentId>> ids =
    docsToDelete
        .onItem()
        .invoke(response -> nextPage[0] = response.pagingState())
        .onItem()
        .transformToMulti(
            findResponse -> {
              return Multi.createFrom().items(findResponse.docs().stream());
            })
        .onItem()
        .transformToUniAndConcatenate(
            readDocument -> deleteDocument(queryExecutor, delete, readDocument))
        .collect()
        .asList();*/
    return ids.onItem().transform(deletedIds -> new DeleteOperationPage(deletedIds, moreData[0]));
  }

  private QueryOuterClass.Query buildDeleteQuery() {
    String delete = "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(delete, commandContext.namespace(), commandContext.collection()))
        .build();
  }

  /**
   * When delete is run with LWT, applied field is always the first field and in case the
   * transaction id mismatch the latest transaction id is returned as second field Eg:
   * cassandra@cqlsh:jsonapi> delete from jsonapi.test1 where key = 'doc2' IF tx_id =
   * 13659a90-9361-11ed-92df-515ba7f99655 ;
   *
   * <p>[applied] | tx_id -----------+-------------------------------------- False |
   * 13659a90-9361-11ed-92df-515ba7f99654
   *
   * <p>cassandra@cqlsh:jsonapi> delete from jsonapi.test1 where key = 'doc2' IF tx_id =
   * 13659a90-9361-11ed-92df-515ba7f99654 ;
   *
   * <p>[applied] ----------- True
   *
   * @param queryExecutor
   * @param query
   * @param doc
   * @return
   */
  private static Uni<DocumentId> deleteDocument(
      QueryExecutor queryExecutor, QueryOuterClass.Query query, ReadDocument doc) {
    query = bindDeleteQuery(query, doc);
    return queryExecutor
        .executeWrite(query)
        .onItem()
        .transformToUni(
            result -> {
              if (result.getRows(0).getValues(0).getBoolean()) {
                return Uni.createFrom().item(doc.id());
              } else {
                return Uni.createFrom().nothing();
              }
            });
  }

  private static QueryOuterClass.Query bindDeleteQuery(
      QueryOuterClass.Query builtQuery, ReadDocument doc) {
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(Values.of(doc.txnId()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }
}
