package io.stargate.sgv3.docsapi.messages;

import com.fasterxml.jackson.databind.JsonNode;

/** NOTE: Unsure on utility, included to make it clear we have request and response. */
public class RequestMessage extends Message {

  public RequestMessage(JsonNode message) {
    super(message);
  }
}
