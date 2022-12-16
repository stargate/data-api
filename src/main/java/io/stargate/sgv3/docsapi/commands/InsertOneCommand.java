package io.stargate.sgv3.docsapi.commands;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Internal representation of the insertOne API {@link Command}.
 *
 * <p>As a Command it should represent the request without using JSON.
 */
public class InsertOneCommand extends ModifyCommand {

  /** This needs to ba a JsonNode, it's the document we want to insert. */
  public final JsonNode document;

  public InsertOneCommand(JsonNode document) {
    this.document = document;
  }

  @Override
  public boolean valid() {
    return false;
  }

  @Override
  public void validate() throws Exception {}
}
