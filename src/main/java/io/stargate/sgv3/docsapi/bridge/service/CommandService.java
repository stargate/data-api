package io.stargate.sgv3.docsapi.bridge.service;

import io.smallrye.mutiny.Uni;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import io.stargate.sgv2.api.common.grpc.GrpcMetadataResolver;
import io.stargate.sgv3.docsapi.commands.Command;
import io.stargate.sgv3.docsapi.commands.CommandContext;
import io.stargate.sgv3.docsapi.commands.CommandResult;
import io.stargate.sgv3.docsapi.commands.serializers.CommandSerializer;
import io.stargate.sgv3.docsapi.messages.ResponseMessage;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationScoped
public class CommandService {
  private static final Logger logger = LoggerFactory.getLogger(CommandService.class);

  @Inject StargateRequestInfo requestInfo;

  @Inject GrpcMetadataResolver metadataResolver;

  @Inject CommandBridgeService commandBridgeService;

  @Inject CommandSerializer commandSerializer;

  public Uni<String> processCommand(String namespace, String collection, String reqMessage) {

    Command command = null;
    Uni<CommandResult> commandResult = null;

    try {
      command = commandSerializer.deserialize(reqMessage);
    } catch (IOException e) {
      // TODO - make sure not leaking full exception stacks
      e.printStackTrace();
      commandResult =
          Uni.createFrom().item(new CommandResult(List.of(), null, Map.of(), List.of(e)));
    }
    // using Mock commands for development
    // Command command = MockCommands.findOne_ById_doc1();

    if (command != null) {
      commandResult =
          commandBridgeService.processCommand(new CommandContext(namespace, collection), command);
    }
    return commandResult
        .onItem()
        .transform(cr -> ResponseMessage.fromCommandResult(cr).message.toPrettyString());
  }
}
