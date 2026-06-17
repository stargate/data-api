package io.stargate.sgv2.jsonapi.service.resolver;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import io.stargate.sgv2.jsonapi.api.model.command.CommandContext;
import io.stargate.sgv2.jsonapi.api.model.command.impl.CreateCollectionCommand;
import io.stargate.sgv2.jsonapi.service.operation.collections.CreateCollectionOperation;
import io.stargate.sgv2.jsonapi.service.schema.KeyspaceSchemaObject;
import jakarta.inject.Inject;

/**
 * re-usable base class for tests for {@link CreateCollectionCommandResolver}.
 */
class CreateCollectionCommandResolverTestBase
    extends CommandResolverTestBase<
        KeyspaceSchemaObject,
        CreateCollectionCommand,
        CreateCollectionCommandResolver,
        CreateCollectionOperation> {

  @Inject CreateCollectionCommandResolver RESOLVER;
  protected final CommandContext<KeyspaceSchemaObject> COMMAND_CONTEXT =
      TEST_CONSTANTS.keyspaceContext();

  @Override
  protected CreateCollectionCommandResolver resolver() {
    return RESOLVER;
  }

  @Override
  protected CommandContext<KeyspaceSchemaObject> commandContext() {
    return COMMAND_CONTEXT;
  }

  @Override
  protected Class<CreateCollectionCommand> commandClass() {
    return CreateCollectionCommand.class;
  }

  /**
   * Run the command json through the resolver using the super class, then some extra common
   * assertions for the createCollection command.
   */
  protected CreateCollectionOperation assertResolver(
      String testName, String rawJson, CqlIdentifier collectionName) {

    var operation = super.assertResolver(testName, rawJson);

    assertThat(operation)
        .isInstanceOfSatisfying(
            CreateCollectionOperation.class,
            op -> {
              assertThat(op.collectionName())
                  .as("%s - collectionName() matches command", testName)
                  .isEqualTo(collectionName);
            });
    return operation;
  }
}
