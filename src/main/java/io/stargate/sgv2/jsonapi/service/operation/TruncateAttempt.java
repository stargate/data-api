package io.stargate.sgv2.jsonapi.service.operation;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CommandQueryExecutor;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An attempt to truncate a table */
public class TruncateAttempt<SchemaT extends TableBasedSchemaObject>
    extends OperationAttempt<TruncateAttempt<SchemaT>, SchemaT> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TruncateAttempt.class);

  public TruncateAttempt(int position, SchemaT schemaObject) {
    super(position, schemaObject, RetryPolicy.NO_RETRY);
    setStatus(OperationStatus.READY);
  }

  @Override
  protected StatementContext buildStatementContext(CommandQueryExecutor queryExecutor) {
    // bind and execute
    var statement = buildTruncateStatement();
    logStatement(LOGGER, "executeStatement()", statement);
    return new StatementContext(statement, () -> queryExecutor.executeTruncate(statement));
  }

  protected SimpleStatement buildTruncateStatement() {

    var metadata = schemaObject.tableMetadata();
    return QueryBuilder.truncate(metadata.getKeyspace(), metadata.getName()).build();
  }
}
