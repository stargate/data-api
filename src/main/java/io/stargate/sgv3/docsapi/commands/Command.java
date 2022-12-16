package io.stargate.sgv3.docsapi.commands;

import io.stargate.sgv3.docsapi.commands.resolvers.CommandResolver;

/**
 * POJO object (data no behavior) that represents a syntactically and grammatically valid command as
 * defined in the API spec.
 *
 * <p>The behavior about *how* to run a Command is in the {@link CommandResolver}.
 *
 * <p>Commands *should not* include JSON other than for documents we want to insert. They should
 * represent a translate of the API request into an internal representation. e.g. this insulates
 * from tweaking JSON on the wire protocol, we would only need to modify how we create the command
 * and nothing else.
 *
 * <p>These may be created from parsing the incoming message and could also be created
 * programmatically for for internal and testing purposes. e.g. make a CreateCollection command in
 * code when we need a new collection because we got a request to a new endpoint, then run the
 * original command
 */
public abstract class Command {

  public abstract boolean valid();

  /**
   * Validates the command is syntactically and grammatically valid.
   *
   * <p>Throw if not valid, otherwise set valid to True
   *
   * @throws Exception
   */
  public abstract void validate() throws Exception;
}
