package io.stargate.sgv2.jsonapi.service.operation.model.impl;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import io.stargate.sgv2.jsonapi.service.operation.model.Operation;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the truncate collection.
 *
 * @param context Command context, carries namespace and the name of the collection.
 */
public record TruncateCollectionOperation(CommandContext context) implements Operation {
  private static final Logger logger = LoggerFactory.getLogger(TruncateCollectionOperation.class);
  private static final String TRUNCATE_TABLE_CQL = "TRUNCATE TABLE \"%s\".\"%s\";";

  @Override
  public Uni<Supplier<CommandResult>> execute(QueryExecutor queryExecutor) {
    logger.info("Executing TruncateCollectionOperation for {}", context.collection());
    String cql = TRUNCATE_TABLE_CQL.formatted(context.namespace(), context.collection());
    SimpleStatement query = SimpleStatement.newInstance(cql);
    // execute
    return queryExecutor
        .executeTruncateSchemaChange(query)
        .onFailure().invoke(e->logger.error("here" + e)).onFailure()
        .retry()
        .atMost(4)
        .onItem()
        .transformToUni(
            any -> {
              // if we have a result, always respond positively
              return Uni.createFrom().item(() -> new DeleteOperationPage(null, false, false));
            });
  }
}
