package io.stargate.sgv2.jsonapi.service.operation;

import io.stargate.sgv2.jsonapi.api.model.command.CommandResultBuilder;
import io.stargate.sgv2.jsonapi.api.model.command.CommandStatus;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.CqlPagingState;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.KeyspaceSchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableBasedSchemaObject;
import java.util.*;

/**
 * A page of results from a list table command, use {@link #builder()} to get a builder to pass to {@link
 * GenericOperation}.
 */
public abstract class MetadataAttemptPage<SchemaT extends KeyspaceSchemaObject>
        extends OperationAttemptPage<SchemaT, MetadataAttempt<SchemaT>> {


  private MetadataAttemptPage(
          OperationAttemptContainer<SchemaT, MetadataAttempt<SchemaT>> attempts,
          CommandResultBuilder resultBuilder) {
    super(attempts, resultBuilder);
  }

  @Override
  protected void buildCommandResult() {

  }
}
