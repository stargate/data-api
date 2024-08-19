package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.*;

/**
 * Interface for any clause of a {@link Command} to implement if it can be validated against a
 * {@link SchemaObject}.
 *
 * <p>Code that wants to validate a clause should call {@link #maybeValidate(CommandContext,
 * ValidatableCommandClause)} with the clause.
 *
 * <p>Example:
 *
 * <pre>
 *  ValidatableCommandClause.maybeValidate(commandContext, command.filterClause());
 * </pre>
 */
public interface ValidatableCommandClause {

  /**
   * Calls the supplied validatable clause to validate against the {@link SchemaObject} from the
   * {@link CommandContext} using one of the dedicated validate*Command methods on the interface.
   *
   * <p>NOTE: Classes that want to validate a clause should call this method, not the non-static
   * methods on the interface directly.
   *
   * @param commandContext The context the command is running against, including the {@link
   *     SchemaObject}
   * @param validatable An object that implements {@link ValidatableCommandClause}, may be null
   * @param <T> Type of the {@link SchemaObject}
   */
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

  /**
   * Implementations should implement this method if they support validation against a {@link
   * CollectionSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Collections, the default implementation is
   * to fail.
   *
   * @param commandContext {@link CommandContext<CollectionSchemaObject>} to validate against
   */
  default void validateCollectionCommand(CommandContext<CollectionSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Collections, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * TableSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Tables, the default implementation is to
   * fail.
   *
   * @param commandContext {@link CommandContext<TableSchemaObject>} to validate against
   */
  default void validateTableCommand(CommandContext<TableSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Tables, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * KeyspaceSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Keyspaces, the default implementation is
   * to fail.
   *
   * @param commandContext {@link CommandContext<KeyspaceSchemaObject>} to validate against
   */
  default void validateNamespaceCommand(CommandContext<KeyspaceSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Namespaces, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }

  /**
   * Implementations should implement this method if they support validation against a {@link
   * DatabaseSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Databases, the default implementation is
   * to fail.
   *
   * @param commandContext {@link CommandContext<DatabaseSchemaObject>} to validate against
   */
  default void validateDatabaseCommand(CommandContext<DatabaseSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support validating for Databases, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name));
  }
}
