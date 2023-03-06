package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
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
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * This operation method is used for 3 commands findOneAndUpdate, updateOne and updateMany
 *
 * @param commandContext
 * @param readOperation
 * @param documentUpdater
 * @param returnDocumentInResponse
 * @param returnUpdatedDocument
 * @param upsert
 * @param shredder
 * @param updateLimit
 */
public record ReadAndUpdateOperation(
    CommandContext commandContext,
    ReadOperation readOperation,
    DocumentUpdater documentUpdater,
    boolean returnDocumentInResponse,
    boolean returnUpdatedDocument,
    boolean upsert,
    Shredder shredder,
    int updateLimit)
    implements ModifyOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    final AtomicBoolean moreDataFlag = new AtomicBoolean(false);
    final Multi<ReadOperation.FindResponse> findResponses =
        Multi.createBy()
            .repeating()
            .uni(
                () -> new AtomicReference<String>(null),
                stateRef -> {
                  Uni<ReadOperation.FindResponse> docsToUpdate =
                      readOperation().getDocuments(queryExecutor, stateRef.get());
                  return docsToUpdate
                      .onItem()
                      .invoke(findResponse -> stateRef.set(findResponse.pagingState()));
                })
            .whilst(findResponse -> findResponse.pagingState() != null);
    final AtomicInteger matchedCount = new AtomicInteger(0);
    final AtomicInteger modifiedCount = new AtomicInteger(0);

    final Uni<List<UpdatedDocument>> updatedDocuments =
        findResponses
            .onItem()
            .transformToMulti(
                findResponse -> {
                  final List<ReadDocument> docs = findResponse.docs();
                  if (upsert() && docs.size() == 0 && matchedCount.get() == 0) {
                    return Multi.createFrom().item(readOperation().getNewDocument());
                  } else {
                    // Below conditionality is because we read up to deleteLimit +1 record.
                    if (matchedCount.get() + docs.size() <= updateLimit) {
                      matchedCount.addAndGet(docs.size());
                      return Multi.createFrom().items(docs.stream());
                    } else {
                      int needed = updateLimit - matchedCount.get();
                      matchedCount.addAndGet(needed);

                      moreDataFlag.set(true);
                      return Multi.createFrom()
                          .items(findResponse.docs().subList(0, needed).stream());
                    }
                  }
                })
            .concatenate()
            .onItem()
            .transformToUniAndConcatenate(
                readDocument -> {
                  final JsonNode originalDocument =
                      readDocument.txnId() == null ? null : readDocument.document();
                  final boolean isInsert = (originalDocument == null);
                  DocumentUpdater.DocumentUpdaterResponse documentUpdaterResponse =
                      documentUpdater().applyUpdates(readDocument.document().deepCopy(), isInsert);
                  JsonNode updatedDocument = documentUpdaterResponse.document();
                  Uni<DocumentId> updated = Uni.createFrom().nullItem();
                  if (documentUpdaterResponse.modified()) {
                    WritableShreddedDocument writableShreddedDocument =
                        shredder().shred(updatedDocument, readDocument.txnId());
                    updated = updatedDocument(queryExecutor, writableShreddedDocument);
                  }
                  final JsonNode documentToReturn =
                      returnUpdatedDocument ? updatedDocument : originalDocument;
                  return updated
                      .onItem()
                      .ifNotNull()
                      .transform(
                          v -> {
                            if (readDocument.txnId() != null) modifiedCount.incrementAndGet();
                            return new UpdatedDocument(
                                readDocument.id(),
                                readDocument.txnId() == null,
                                returnDocumentInResponse ? documentToReturn : null);
                          });
                })
            .collect()
            .asList();

    return updatedDocuments
        .onItem()
        .transform(
            updates ->
                new UpdateOperationPage(
                    matchedCount.get(),
                    modifiedCount.get(),
                    updates,
                    returnDocumentInResponse(),
                    moreDataFlag.get()));
  }

  private Uni<DocumentId> updatedDocument(
      QueryExecutor queryExecutor, WritableShreddedDocument writableShreddedDocument) {
    final QueryOuterClass.Query updateQuery =
        bindUpdateValues(buildUpdateQuery(), writableShreddedDocument);
    return queryExecutor
        .executeWrite(updateQuery)
        .onItem()
        .transformToUni(
            result -> {
              if (result.getRows(0).getValues(0).getBoolean()) {
                return Uni.createFrom().item(writableShreddedDocument.id());
              } else {
                return Uni.createFrom().nullItem();
              }
            });
  }

  private QueryOuterClass.Query buildUpdateQuery() {
    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            exist_keys = ?,"
            + "            sub_doc_equals = ?,"
            + "            array_size = ?,"
            + "            array_equals = ?,"
            + "            array_contains = ?,"
            + "            query_bool_values = ?,"
            + "            query_dbl_values = ?,"
            + "            query_text_values = ?,"
            + "            query_null_values = ?,"
            + "            doc_json  = ?"
            + "        WHERE "
            + "            key = ?"
            + "        IF "
            + "            tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(update, commandContext.namespace(), commandContext.collection()))
        .build();
  }

  protected static QueryOuterClass.Query bindUpdateValues(
      QueryOuterClass.Query builtQuery, WritableShreddedDocument doc) {
    // respect the order in the DocsApiConstants.ALL_COLUMNS_NAMES
    QueryOuterClass.Values.Builder values =
        QueryOuterClass.Values.newBuilder()
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.existKeys())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.subDocEquals())))
            .addValues(Values.of(CustomValueSerializers.getIntegerMapValues(doc.arraySize())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.arrayEquals())))
            .addValues(Values.of(CustomValueSerializers.getStringSetValue(doc.arrayContains())))
            .addValues(Values.of(CustomValueSerializers.getBooleanMapValues(doc.queryBoolValues())))
            .addValues(
                Values.of(CustomValueSerializers.getDoubleMapValues(doc.queryNumberValues())))
            .addValues(Values.of(CustomValueSerializers.getStringMapValues(doc.queryTextValues())))
            .addValues(Values.of(CustomValueSerializers.getSetValue(doc.queryNullValues())))
            .addValues(Values.of(doc.docJson()))
            .addValues(Values.of(CustomValueSerializers.getDocumentIdValue(doc.id())))
            .addValues(doc.txID() == null ? Values.NULL : Values.of(doc.txID()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }

  record UpdatedDocument(DocumentId id, boolean upserted, JsonNode document) {}
}
