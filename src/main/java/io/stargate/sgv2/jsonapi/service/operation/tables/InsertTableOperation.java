package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.google.common.base.Preconditions;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.InsertOperationPage;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.JSONCodecRegistry;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.MissingJSONCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.ToCQLCodecException;
import io.stargate.sgv2.jsonapi.service.operation.filters.table.codecs.UnknownColumnException;
import io.stargate.sgv2.jsonapi.service.shredding.tables.WriteableTableRow;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertTableOperation extends TableMutationOperation {
  private static final Logger LOGGER = LoggerFactory.getLogger(InsertTableOperation.class);

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

    // TODO AARON - this is for unordered, copy from Collection InsertCollectionOperation
    // insertUnordered
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

    // If we did not fail, then we should have a row, test that
    if (insertAttempt.row().isEmpty()){
      return Uni.createFrom().failure(new IllegalStateException("InsertAttempt has no row, and no failure"));
    }

    // bind and execute
    var boundStatement = buildInsertStatement(queryExecutor, insertAttempt);

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

  private SimpleStatement buildInsertStatement(QueryExecutor queryExecutor, TableInsertAttempt insertAttempt) {

    InsertInto insertInto = insertInto(
            commandContext.schemaObject().tableMetadata.getKeyspace(),
            commandContext.schemaObject().tableMetadata.getName());

    List<Object> positionalValues = new ArrayList<>();
    RegularInsert ongoingInsert = insertAttempt.getInsertValuesBuilder().apply(insertInto, positionalValues);
    return SimpleStatement.newInstance(ongoingInsert.asCql(), positionalValues.toArray());
  }
}
