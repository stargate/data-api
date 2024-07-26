package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.operation.ReadOperationPage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

/**
 * TODO: this is still a POC class, showing how we can build a filter still to do is order and
 * projections
 */
public class FindTableOperation extends TableReadOperation {

  private final SelectBuilder selectBuilder;
  private final WhereBuilder whereBuilder;
  private final DocumentSourceSupplier documentSourceSupplier;
  private final FindTableParams params;

  public FindTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      SelectBuilder selectBuilder,
      WhereBuilder whereBuilder,
      DocumentSourceSupplier documentSourceSupplier,
      FindTableParams params) {
    super(commandContext );

    this.selectBuilder = Objects.requireNonNull(selectBuilder, "selectBuilder must not be null");
    this.whereBuilder = Objects.requireNonNull(whereBuilder, "whereBuilder must not be null");
    this.documentSourceSupplier = Objects.requireNonNull(documentSourceSupplier, "documentSourceSupplier must not be null");
    this.params = Objects.requireNonNull(params, "params must not be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // Start the select
    SelectFrom selectFrom = selectFrom(
        commandContext.schemaObject().tableMetadata.getKeyspace(),
        commandContext.schemaObject().tableMetadata.getName());

    // Add the columns we want to select
    Select select = selectBuilder.apply(selectFrom);

    // Add the where clause
    List<Object> positionalValues = new ArrayList<>();
    select = whereBuilder.apply(select, positionalValues);

    // Add things like limit
    select = params.options().apply(select);

    var statement = select.build(positionalValues.toArray());

    // TODO: pageSize for FindTableOperation
    return queryExecutor
        .executeRead(dataApiRequestInfo, statement, Optional.empty(), 100)
        .onItem()
        .transform(this::toReadOperationPage);
  }

  private ReadOperationPage toReadOperationPage(AsyncResultSet resultSet) {

    var docSources =
        StreamSupport.stream(resultSet.currentPage().spliterator(), false)
            .map(documentSourceSupplier::documentSource)
            .toList();

    return new ReadOperationPage(docSources, params.isSingleResponse(), null, false, null);
  }

  public record FindTableParams(int limit) {

    public FindTableParams(int limit) {
      // TODO, refactor all Guava checks
      Preconditions.checkArgument(limit > 0, "Limit must be greater than 0");
      this.limit = limit;
    }

    public boolean isSingleResponse() {
      return limit == 1;
    }

    public OptionsBuilder options() {
      return new OptionsBuilder(){
        @Override
        public Select apply(Select select) {
          return select.limit(limit());
        }
      };
    }
  }
}
