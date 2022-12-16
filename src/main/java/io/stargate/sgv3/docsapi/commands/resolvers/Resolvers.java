package io.stargate.sgv3.docsapi.commands.resolvers;

import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CreateCollectionCommand;
import io.stargate.sgv3.docsapi.commands.FindCommand;
import io.stargate.sgv3.docsapi.commands.FindOneAndUpdateCommand;
import io.stargate.sgv3.docsapi.commands.FindOneCommand;
import io.stargate.sgv3.docsapi.commands.InsertOneCommand;
import io.stargate.sgv3.docsapi.commands.UpdateOneCommand;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility to map from {@link Command} to @link {CommandResolver}
 *
 * <p>NOTE: Every resolver must be regsitered here. There will be better ways to do this
 */
public class Resolvers {

  // HACK - this is an easy way to register resolvers for commands
  private static final Map<Class<?>, CommandResolver<? extends Command>> resolvers;

  static {
    resolvers = new HashMap<>();
    resolvers.put(FindOneCommand.class, new FindOneCommandResolver());
    resolvers.put(InsertOneCommand.class, new InsertOneCommandResolver());
    resolvers.put(UpdateOneCommand.class, new UpdateOneCommandResolver());
    resolvers.put(FindOneAndUpdateCommand.class, new FindOneAndUpdateCommandResolver());
    resolvers.put(FindCommand.class, new FindCommandResolver());
    resolvers.put(CreateCollectionCommand.class, new CreateCollectionResolver());
  }

  public static <T extends Command> CommandResolver<T> resolverForCommand(T command) {

    if (resolvers.containsKey(command.getClass())) {
      return (CommandResolver<T>) resolvers.get(command.getClass());
    }
    throw new RuntimeException(
        String.format("No resolver for command class %s", command.getClass()));
  }
}
