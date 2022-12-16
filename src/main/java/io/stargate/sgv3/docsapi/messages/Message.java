package io.stargate.sgv3.docsapi.messages;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * POJO object (data no behavior) that represents the raw request message as we have received or
 * returned through the API.
 *
 * <p>We may end up with "headers" on this to include tenant, auth etc. Not sure.
 */
public abstract class Message {
  public final JsonNode message;

  protected Message(JsonNode message) {
    this.message = message;
  }

  @Override
  public String toString() {
    return String.format(
        "%s Message with message %s", getClass().toString(), message.toPrettyString());
  }
}
