package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.operations.Operation;

/**
 * Resolver looks at a valid {@link Command} and determines the best {@link Operation} to implement
 * the command, creates and configures it. This is the behavior of the command (how we will read or
 * insert the data) separated from the data in the Command object.
 *
 * <p>There is one resolver per Command subtype.
 *
 * <p>e.g. FindOneCommandResolver may result in a FindByIdOperation if the filter is just on "_id"
 * or a FindByOneStringOperation if selecting by username.
 *
 * <p>This behavior is in the resolver because we want the Commands to be dumb POJO that simply
 * represent an intention to do something, not how to do it. We can then store, replay etc commands.
 *
 * <p>It is possible for there to be multiple ways to resolve the query, the resolver should then
 * pick the one with the lowest "cost". e.g. if we can do it with a pushdown query do that. This is
 * still to be defined.
 *
 * <p>There is 8 to 10 different commands in the API
 *
 * <p>*NOTE:* AS we add more C* database capabilities, such as better SAI, we create / update
 * Operations to use them and then update Resolvers so the Commands use the new DB Operations.
 *
 * <p>T - The subtype of Command this resolver works with
 */
public interface CommandResolver<T extends Command> {

  /**
   * Implementations should return a {@link Operation} to execute the command that has the {@link
   * CommandContext} so it knows the tenant, db, etc and has all the data it needs.
   *
   * @param commandContext
   * @param command
   * @return
   */
  public Operation resolveCommand(CommandContext commandContext, T command);

  public static UnresolvedCommandException getUnresolvedCommandException(Command command) {
    throw new UnresolvedCommandException(command);
  }

  public static class UnresolvedCommandException extends RuntimeException {
    public final Command command;

    public UnresolvedCommandException(Command command) {
      super(String.format("Could not resolve command %s", command.toString()));
      this.command = command;
    }
  }
}
