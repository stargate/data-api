package io.stargate.sgv3.docsapi.messages;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.sgv3.docsapi.commands.CommandResult;

/** NOTE: Unsure on utility, included to make it clear we have request and response. */
public class ResponseMessage extends Message {

  private ResponseMessage(JsonNode jsonNode) {
    super(jsonNode);
  }

  /**
   * The {@link CommandResult} has already de-shredded for us, the task now is to frame the response
   * message.
   *
   * <p>e.g. if there is errors we may want to only have errors in the response message, this needs
   * to be decided in spec just the sort of task here.
   *
   * @param commandResult
   * @return
   */
  public static ResponseMessage fromCommandResult(CommandResult commandResult) {

    // TODO : reuse mappers
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.valueToTree(commandResult);
    return new ResponseMessage(jsonNode);
  }
}
