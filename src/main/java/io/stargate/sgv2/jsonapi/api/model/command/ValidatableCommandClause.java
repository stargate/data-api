package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;

public interface ValidatableCommandClause {

  static <T extends SchemaObject> void maybeValidate(
      CommandContext<T> commandContext, ValidatableCommandClause validatable) {
    if (validatable == null) {
      return;
    }

    switch (commandContext.schemaObject().type) {
      case COLLECTION:
        validatable.validateCollectionCommand(commandContext.asCollectionContext());
        break;
      case TABLE:
        validatable.validateTableCommand(commandContext.asTableContext());
        break;
      case KEYSPACE:
        validatable.validateNamespaceCommand(commandContext.asKeyspaceContext());
        break;
      case DATABASE:
        validatable.validateDatabaseCommand(commandContext.asDatabaseContext());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported schema type: %s", commandContext.schemaObject().type));
    }
  }

  default void validateCollectionCommand(CommandContext<CollectionSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Collections, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  default void validateTableCommand(CommandContext<TableSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Tables, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  default void validateNamespaceCommand(CommandContext<KeyspaceSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Namespaces, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  default void validateDatabaseCommand(CommandContext<DatabaseSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Databases, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }
}
