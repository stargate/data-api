package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation.FindResponse;
import io.stargate.sgv2.jsonapi.service.shredding.Shredder;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv2.jsonapi.service.shredding.model.WritableShreddedDocument;
import io.stargate.sgv2.jsonapi.service.updater.DocumentUpdater;
import io.stargate.sgv3.docsapi.service.sequencer.QueryOptions;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequenceSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public record ReadAndUpdateOperation(
    CommandContext commandContext,
    ReadOperation readOperation,
    DocumentUpdater documentUpdater,
    boolean returnDoc,
    Shredder shredder)
    implements ModifyOperation {

  @Override
  public QuerySequenceSink<Supplier<CommandResult>> getOperationSequence() {
    QuerySequence<FindResponse> documentsSequence = readOperation.getDocumentsSequence();

    // execute document sequence from read op
    return documentsSequence

        // then consume results
        .then()
        .pipeToSink(
            findResponse -> {
              // documents from read op
              List<ReadDocument> documents = findResponse.docs();

              // create a list of updated queries
              // build update query
              QueryOuterClass.Query updateQuery = buildUpdateQuery();

              // create update query per document
              List<QueryOuterClass.Query> queries = new ArrayList<>();
              List<JsonNode> updatedNodes = new ArrayList<>();
              for (ReadDocument document : documents) {
                JsonNode updated = documentUpdater().applyUpdates(document.document().deepCopy());
                updatedNodes.add(updated);

                WritableShreddedDocument writableDocument =
                    shredder().shred(updated, document.txnId());
                QueryOuterClass.Query query = bindUpdateValues(updateQuery, writableDocument);
                queries.add(query);
              }

              // execute all updates as write
              return QuerySequence.queries(queries, QueryOptions.Type.WRITE)

                  // handler in case no exception and applied, return UpdatedDocument
                  .<Optional<UpdatedDocument>>withHandler(
                      (result, throwable, index) -> {
                        if (null == throwable) {
                          boolean applied = result.getRows(0).getValues(0).getBoolean();
                          if (applied) {
                            ReadDocument readDocument = documents.get(index);
                            JsonNode originalNode = readDocument.document();
                            JsonNode updatedNode = updatedNodes.get(index);
                            return Optional.of(
                                new UpdatedDocument(readDocument.id(), originalNode));
                          }
                        }
                        return Optional.empty();
                      })

                  // sink results to the UpdateOperationPage
                  .sink(
                      results -> {
                        List<UpdatedDocument> updates =
                            results.stream()
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .toList();

                        return new UpdateOperationPage(updates, returnDoc);
                      });
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
        .setCql(String.format(update, commandContext.database(), commandContext.collection()))
        .build();
  }

  private static QueryOuterClass.Query bindUpdateValues(
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
