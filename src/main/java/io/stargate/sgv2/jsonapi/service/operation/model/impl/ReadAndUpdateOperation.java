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
import java.util.function.Supplier;

public record ReadAndUpdateOperation(
    CommandContext commandContext,
    ReadOperation readOperation,
    DocumentUpdater documentUpdater,
    boolean returnDoc,
    Shredder shredder)
    implements ModifyOperation {

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    Uni<ReadOperation.FindResponse> docsToUpate = readOperation().getDocuments(queryExecutor);
    final Uni<List<UpdatedDocument>> updatedDocuments =
        docsToUpate
            .onItem()
            .transformToMulti(
                findResponse -> Multi.createFrom().items(findResponse.docs().stream()))
            .onItem()
            .transformToUniAndConcatenate(
                readDocument -> {
                  JsonNode originalDocument = readDocument.document().deepCopy();
                  JsonNode updatedDocument =
                      documentUpdater().applyUpdates(readDocument.document());
                  WritableShreddedDocument writableShreddedDocument =
                      shredder().shred(updatedDocument, readDocument.txnId());
                  return updatedDocument(queryExecutor, writableShreddedDocument)
                      .onItem()
                      .transform(v -> new UpdatedDocument(readDocument.id(), originalDocument));
                })
            .collect()
            .asList();
    return updatedDocuments
        .onItem()
        .transform(updates -> new UpdateOperationPage(updates, returnDoc()));
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
                return Uni.createFrom().nothing();
              }
            });
  }

  private QueryOuterClass.Query buildUpdateQuery() {
    String update =
        "UPDATE %s.%s "
            + "        SET"
            + "            tx_id = now(),"
            + "            doc_properties = ?,"
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
            .addValues(Values.of(CustomValueSerializers.getIntegerMapValues(doc.docProperties())))
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
            .addValues(Values.of(doc.txID()));
    return QueryOuterClass.Query.newBuilder(builtQuery).setValues(values).build();
  }

  record UpdatedDocument(DocumentId id, JsonNode document) {}
}
