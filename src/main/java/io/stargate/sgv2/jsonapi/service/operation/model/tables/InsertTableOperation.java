package io.stargate.sgv2.jsonapi.service.operation.model.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.model.InsertOperationPage;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class InsertTableOperation extends TableMutationOperation {

  private final List<TableInsertAttempt> insertAttempts;

  // TODO AARON JSON to start with, need a document object
  public InsertTableOperation(
      CommandContext<TableSchemaObject> commandContext, List<TableInsertAttempt> insertAttempts) {
    super(commandContext);
    this.insertAttempts = List.copyOf(insertAttempts);
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // TODO AARON - this is for unordered, copy from Collection InsertOperation insertUnordered
    return Multi.createFrom()
        .iterable(insertAttempts)
        // merge to make it parallel
        .onItem()
        .transformToUniAndMerge(
            insertion -> insertRow(dataApiRequestInfo, queryExecutor, insertion))
        // then reduce here
        .collect()
        .in(() -> new InsertOperationPage(insertAttempts, false), InsertOperationPage::aggregate)
        // use object identity to resolve to Supplier<CommandResult>
        // TODO AARON - not sure what this is doing, original was .map(i -> i)
        .map(Function.identity());
  }

  private Uni<TableInsertAttempt> insertRow(
      DataApiRequestInfo dataApiRequestInfo,
      QueryExecutor queryExecutor,
      TableInsertAttempt insertAttempt) {

    // First things first: did we already fail? If so, propagate
    if (insertAttempt.failure().isPresent()) {
      return Uni.createFrom().failure(insertAttempt.failure().get());
    }

    // bind and execute
    var boundStatement = buildInsertStatement(queryExecutor, insertAttempt.row().orElseThrow());

    // TODO: AARON What happens to errors here?
    return queryExecutor
        .executeWrite(dataApiRequestInfo, boundStatement)
        .onItemOrFailure()
        .transform(
            (result, t) -> {
              if (t != null) {
                return (TableInsertAttempt) insertAttempt.maybeAddFailure(t);
              }
              // This is where to check result.wasApplied() if this was a LWT
              return insertAttempt;
            })
        .onItemOrFailure()
        .transform((ia, throwable) -> (TableInsertAttempt) ia.maybeAddFailure(throwable));
  }

  private SimpleStatement buildInsertStatement(QueryExecutor queryExecutor, WriteableTableRow row) {

    Map<CqlIdentifier, Term> colValues =
        row.allColumnValues().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> literal(e.getValue())));

    return insertInto(
            commandContext.schemaObject().name.keyspace(),
            commandContext.schemaObject().name.table())
        .valuesByIds(colValues)
        .build();
  }
}
