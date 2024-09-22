package io.stargate.sgv2.jsonapi.service.operation.tables;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.google.common.base.Preconditions;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.api.request.RequestContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.*;
import java.util.*;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO: this is still a POC class, showing how we can build a filter still to do is order and
 * projections
 */
public class FindTableOperation<SchemaT extends TableBasedSchemaObject> extends TableReadOperation {

  private static final Logger LOGGER = LoggerFactory.getLogger(TableReadOperation.class);

  private final DriverExceptionHandler<SchemaT> driverExceptionHandler;
  private final List<ReadAttempt<SchemaT>> readAttempts;

  private final ReadAttemptPage.Builder<SchemaT> pageBuilder;

  public FindTableOperation(
      CommandContext<TableSchemaObject> commandContext,
      DriverExceptionHandler<SchemaT> driverExceptionHandler,
      List<? extends ReadAttempt<SchemaT>> readAttempts,
      ReadAttemptPage.Builder<SchemaT> pageBuilder) {
    super(commandContext);

    this.driverExceptionHandler =
        Objects.requireNonNull(driverExceptionHandler, "driverExceptionHandler cannot be null");
    this.readAttempts =
        List.copyOf(Objects.requireNonNull(readAttempts, "readAttempts cannot be null"));
    this.pageBuilder = Objects.requireNonNull(pageBuilder, "pageBuilder cannot be null");
  }

  @Override
  public Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor) {

    // TODO AARON - for now we create the CommandQueryExecutor here , later change the Operation
    // interface
    CommandQueryExecutor commandQueryExecutor =
        new CommandQueryExecutor(
            queryExecutor.getCqlSessionCache(),
            new RequestContext(
                dataApiRequestInfo.getTenantId(), dataApiRequestInfo.getCassandraToken()),
            CommandQueryExecutor.QueryTarget.TABLE);

    return Multi.createFrom()
        .iterable(readAttempts)
        .onItem()
        .transformToUniAndMerge(
            readAttempt -> readAttempt.execute(commandQueryExecutor, driverExceptionHandler))
        .onItem()
        .transform(OperationAttempt::setSkippedIfReady)
        .collect()
        .in(() -> pageBuilder, OperationAttemptAccumulator::accumulate)
        .onItem()
        .transform(ReadAttemptPage.Builder::build);
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
      return new OptionsBuilder() {
        @Override
        public Select apply(Select select) {
          return select.limit(limit());
        }
      };
    }
  }
}
