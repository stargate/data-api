package io.stargate.sgv2.jsonapi.service.operation.collections;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Reads a bounded set of candidates for reranking, following continuation pages as needed. */
public record CollectionCandidateReadOperation(FindCollectionOperation readOperation)
    implements CollectionReadOperation {

  public CollectionCandidateReadOperation {
    Objects.requireNonNull(readOperation, "readOperation");
    if (readOperation.limit() <= 0) {
      throw new IllegalArgumentException("Candidate reads require a positive limit");
    }
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      RequestContext requestContext, QueryExecutor queryExecutor) {
    return readOperation.executeRead(() -> getDocuments(requestContext, queryExecutor));
  }

  private Uni<FindResponse> getDocuments(
      RequestContext requestContext, QueryExecutor queryExecutor) {
    return switch (readOperation.readType()) {
      case SORTED_DOCUMENT ->
          readOperation.getSortedDocuments(requestContext, queryExecutor, null, true);
      case DOCUMENT, KEY -> findCandidateDocuments(requestContext, queryExecutor);
      default ->
          Uni.createFrom()
              .failure(
                  new IllegalArgumentException(
                      "Unsupported candidate read type `%s`".formatted(readOperation.readType())));
    };
  }

  private Uni<FindResponse> findCandidateDocuments(
      RequestContext requestContext, QueryExecutor queryExecutor) {
    var queries = readOperation.buildSelectQueries(null);
    var commandContext = readOperation.commandContext();
    return Uni.createFrom()
        .deferred(
            () -> {
              List<ReadDocument> documents = new ArrayList<>();
              return Multi.createFrom()
                  .items(queries.stream())
                  .onItem()
                  .transformToUniAndConcatenate(
                      query -> {
                        if (documents.size() >= readOperation.limit()) {
                          return Uni.createFrom().voidItem();
                        }
                        return Multi.createBy()
                            .repeating()
                            .uni(
                                () -> new AtomicReference<>(readOperation.pageState()),
                                state -> {
                                  Uni<AsyncResultSet> result =
                                      readOperation.vector() != null
                                          ? queryExecutor.executeVectorSearch(
                                              requestContext,
                                              query,
                                              Optional.ofNullable(state.get()),
                                              readOperation.pageSize())
                                          : queryExecutor.executeRead(
                                              requestContext,
                                              query,
                                              Optional.ofNullable(state.get()),
                                              readOperation.pageSize());
                                  return result.map(
                                      rSet -> {
                                        FindResponse response =
                                            parseDocumentPage(
                                                rSet,
                                                readOperation.readType()
                                                    == CollectionReadType.DOCUMENT,
                                                readOperation.objectMapper(),
                                                readOperation.projection(),
                                                readOperation.limit() - documents.size(),
                                                commandContext.requestContext().tenant(),
                                                commandContext.commandName(),
                                                commandContext.jsonProcessingMetricsReporter());
                                        documents.addAll(response.docs());
                                        state.set(response.pageState());
                                        return response;
                                      });
                                })
                            .whilst(
                                response ->
                                    documents.size() < readOperation.limit()
                                        && response.pageState() != null)
                            .collect()
                            .last()
                            .replaceWithVoid();
                      })
                  .collect()
                  .asList()
                  .map(ignored -> new FindResponse(documents, null));
            });
  }
}
