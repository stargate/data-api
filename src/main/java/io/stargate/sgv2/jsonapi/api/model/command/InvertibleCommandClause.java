package io.stargate.sgv2.jsonapi.api.model.command;

import io.stargate.sgv2.jsonapi.service.cqldriver.executor.SchemaObject;
import io.stargate.sgv2.jsonapi.service.cqldriver.executor.TableSchemaObject;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionSchemaObject;

/**
 * Interface for filter clause to implement if it can be inverted
 *
 * <p>Code that wants to invert a clause should call {@link #maybeInvert(CommandContext,
 * InvertibleCommandClause)} with the clause.
 */
public interface InvertibleCommandClause {

  /**
   * Calls the supplied invertible clause to invert against the {@link SchemaObject} from the {@link
   * CommandContext} using one of the dedicated invert*Command methods on the interface.
   *
   * <p>NOTE: Classes that want to invertible a clause should call this method, not the non-static
   * methods on the interface directly.
   *
   * @param commandContext The context the command is running against, including the {@link
   *     SchemaObject}
   * @param invertible An object that implements {@link InvertibleCommandClause}, may be null
   * @param <T> Type of the {@link SchemaObject}
   */
  static <T extends SchemaObject> void maybeInvert(
      CommandContext<T> commandContext, InvertibleCommandClause invertible) {
    if (invertible == null) {
      return;
    }
    switch (commandContext.schemaObject().type()) {
      case COLLECTION:
        invertible.invertForCollectionCommand(commandContext.asCollectionContext());
        break;
      case TABLE:
        invertible.invertForTableCommand(commandContext.asTableContext());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported schema type: %s", commandContext.schemaObject().type()));
    }
  }

  /**
   * Implementations should implement this method if they support invert against a {@link
   * CollectionSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Collections, the default implementation is
   * to fail.
   *
   * @param commandContext {@link CommandContext<CollectionSchemaObject>} to invert against
   */
  default void invertForCollectionCommand(CommandContext<CollectionSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support invert for Collections, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name()));
  }

  /**
   * Implementations should implement this method if they support invert against a {@link
   * TableSchemaObject}.
   *
   * <p>Only implement this method if the clause supports Tables, the default implementation is to
   * fail.
   *
   * @param commandContext {@link CommandContext<TableSchemaObject>} to invert against
   */
  default void invertForTableCommand(CommandContext<TableSchemaObject> commandContext) {
    // there error is a fallback to make sure it is implemented if it should be
    // commands are tested well
    throw new UnsupportedOperationException(
        String.format(
            "%s Clause does not support invert for Tables, target was %s",
            getClass().getSimpleName(), commandContext.schemaObject().name()));
  }
}
