package io.stargate.sgv2.jsonapi.api.model.command;

/**
 * Defines the context in which to execute the command.
 *
 * @param database The name of the database.
 * @param collection The name of the collection.
 */
public record CommandContext(String database, String collection) {

  private static final CommandContext EMPTY = new CommandContext(null, null);

  /**
   * @return Returns empty command context, having both {@link #database} and {@link #collection} as
   *     <code>null</code>.
   */
  public static CommandContext empty() {
    return EMPTY;
  }
}
