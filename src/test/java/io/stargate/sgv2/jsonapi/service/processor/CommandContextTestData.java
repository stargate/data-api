package io.stargate.sgv2.jsonapi.service.processor;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;
import io.stargate.sgv2.jsonapi.service.operation.tasks.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;

/** tests data and mocks for working with {@link CommandContext} */
public class CommandContextTestData {

  public <T extends SchemaObject> CommandContext<T> mockCommandContext(T schemaObject) {

    CommandContext<T> commandContextMock = mock(CommandContext.class);

    when(commandContextMock.schemaObject()).thenReturn(schemaObject);
    when(commandContextMock.asDatabaseContext())
        .thenReturn((CommandContext<DatabaseSchemaObject>) commandContextMock);
    when(commandContextMock.asKeyspaceContext())
        .thenReturn((CommandContext<KeyspaceSchemaObject>) commandContextMock);
    when(commandContextMock.asCollectionContext())
        .thenReturn((CommandContext<CollectionSchemaObject>) commandContextMock);
    when(commandContextMock.asTableContext())
        .thenReturn((CommandContext<TableSchemaObject>) commandContextMock);
    return commandContextMock;
  }
}
