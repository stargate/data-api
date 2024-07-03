package io.stargate.sgv2.jsonapi.service.resolver;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.jsonapi.api.model.command.Command;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.service.resolver.model.CommandResolver;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Service to map each available {@link CommandResolver}. */
@ApplicationScoped
public class CommandResolverService {

  private final Map<Class<? extends Command>, CommandResolver<? extends Command>> resolvers;

  @Inject
  public CommandResolverService(Instance<CommandResolver<? extends Command>> commandResolvers) {
    this(commandResolvers.stream().toList());
  }

  public CommandResolverService(List<CommandResolver<? extends Command>> commandResolvers) {
    this.resolvers = new HashMap<>();
    if (null != commandResolvers) {
      commandResolvers.forEach(resolver -> resolvers.put(resolver.getCommandClass(), resolver));
    }
  }

  /**
   * @param command {@link Command}
   * @return Emits the {@link CommandResolver} for the given command if one exists, otherwise emits
   *     a null item.
   * @param <T> Type of the command
   */
  public <T extends Command> Uni<CommandResolver<T>> resolverForCommand(T command) {
    // try to get from the map of resolvers
    CommandResolver<T> resolver = (CommandResolver<T>) resolvers.get(command.getClass());
    return Uni.createFrom()
        .item(resolver)
        // if this results to null, fail here with not implemented
        .onItem()
        .ifNull()
        .failWith(
            () -> {
              // Should never happen: all Commands should have matching resolvers
              return ErrorCode.SERVER_INTERNAL_ERROR.toApiException(
                  "No `CommandResolver` for Command \"%s\"", command.getClass().getName());
            });
  }
}
