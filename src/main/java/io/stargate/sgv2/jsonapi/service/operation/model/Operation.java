package io.stargate.sgv2.jsonapi.service.operation.model;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.CommandResult;
import io.stargate.sgv2.jsonapi.api.request.DataApiRequestInfo;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.QueryExecutor;
import java.util.function.Supplier;

/**
 * Operations get data in and out of Cassandra. Some are very specific by find on doc by ID, or
 * insert one doc. Others may be more general like filtering by several text fields.
 *
 * <p>Operations may be able to push down to the database (this is good) but some will may need to
 * filter or sort in memory (not as good). In general good things are good.
 *
 * <p>Operations are built with all the data they need to execute, and only know about standard Java
 * types and our shredded document. That is they do not interact with JSON. As well as data they
 * also have all the behavior they need to implement the operation.
 */
public interface Operation {
  Uni<Supplier<CommandResult>> execute(
      DataApiRequestInfo dataApiRequestInfo, QueryExecutor queryExecutor);
}
