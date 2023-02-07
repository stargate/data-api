package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import io.stargate.bridge.grpc.Values;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.bridge.serializer.CustomValueSerializers;
import io.stargate.sgv2.jsonapi.service.operation.model.ModifyOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation;
import io.stargate.sgv2.jsonapi.service.operation.model.ReadOperation.FindResponse;
import io.stargate.sgv2.jsonapi.service.shredding.model.DocumentId;
import io.stargate.sgv3.docsapi.service.sequencer.QueryOptions;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequence;
import io.stargate.sgv3.docsapi.service.sequencer.QuerySequenceSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Executes readOperation to get the documents ids based on filter condition. All the ids are
 * deleted as LWT based on the id and tx_id.
 */
public record DeleteOperation(CommandContext commandContext, ReadOperation readOperation)
    implements ModifyOperation {

  /**
   * {@inheritDoc}
   *
   * <p>When delete is run with LWT, applied field is always the first field and in case the
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
   */
  @Override
  public QuerySequenceSink<Supplier<CommandResult>> getOperationSequence() {
    QueryOuterClass.Query delete = buildDeleteQuery();
    QuerySequence<FindResponse> documentsSequence = readOperation().getDocumentsSequence();

    // execute document sequence from read op
    return documentsSequence

        // then consume results
        .then()
        .pipeToSink(
            findResponse -> {
              // go through found docs and transform to the delete query for each
              List<ReadDocument> documents = findResponse.docs();

              // create update query per document
              List<QueryOuterClass.Query> queries = new ArrayList<>();
              for (ReadDocument document : documents) {
                QueryOuterClass.Query query = bindDeleteQuery(delete, document);
                queries.add(query);
              }

              // create next sequence
              return QuerySequence.queries(queries, QueryOptions.Type.WRITE)

                  // add handler that returns docs id for success
                  .<Optional<DocumentId>>withHandler(
                      (result, throwable, index) -> {
                        if (null == throwable) {
                          boolean applied = result.getRows(0).getValues(0).getBoolean();
                          if (applied) {
                            ReadDocument doc = documents.get(index);
                            return Optional.of(doc.id());
                          }
                        }
                        return Optional.empty();
                      })

                  // sink that to result
                  .sink(
                      results -> {
                        List<DocumentId> deletedIds =
                            results.stream()
                                .filter(Optional::isPresent)
                                .map(Optional::get)
                                .toList();
                        return new DeleteOperationPage(deletedIds);
                      });
            });
  }

  private QueryOuterClass.Query buildDeleteQuery() {
    String delete = "DELETE FROM \"%s\".\"%s\" WHERE key = ? IF tx_id = ?";
    return QueryOuterClass.Query.newBuilder()
        .setCql(String.format(delete, commandContext.database(), commandContext.collection()))
        .build();
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
