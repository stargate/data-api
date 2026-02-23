package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;

import org.apache.commons.text.StringSubstitutor;

public class TestCommand  {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ObjectNode request;
  private final CommandName commandName;
  private final String includeFrom;

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public TestCommand(ObjectNode request) {

    // if non null, that this is a point to find commands in the named test.
    var includeField =request.get("$include");
    this.includeFrom = includeField == null ? null : includeField.asText();

    if (includeField != null) {
      this.request = null;
      this.commandName = null;
    }
    else {
      this.request = request;
      this.commandName = commandName(request);
    }
  }

  private void checkIsInclude(){
    if (includeFrom != null) {
      throw new IllegalStateException("TestCommand is defined to $include from: " + includeFrom);
    }
  }
  public ObjectNode request() {
    checkIsInclude();
    return request;
  }
  public CommandName commandName() {
    checkIsInclude();
    return commandName;
  }

  public String includeFrom() {
    return includeFrom;
  }

  public static TestCommand fromJson(String json) {

    try {
      return  new TestCommand((ObjectNode) OBJECT_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static CommandName commandName(ObjectNode request) {
    var requestCommandName = commandNameString(request);
    for (CommandName name : CommandName.values()) {
      if (name.getApiName().equals(requestCommandName)) {
        return name;
      }
    }
    throw new IllegalArgumentException("Unknown command name: " + requestCommandName);
  }

  private static String commandNameString(ObjectNode request) {
    var it = request.fieldNames();
    if (!it.hasNext()) {
      throw new IllegalStateException("Expected exactly one field, found none");
    }
    String name = it.next();
    if (it.hasNext()) {
      throw new IllegalStateException("Expected exactly one field, found multiple");
    }
    return name;
  }

  public ObjectNode withEnvironment(TestEnvironment env) {
    ObjectNode updated = request.deepCopy();
    walk(updated, env.substitutor());
    return updated;
  }


  private static void walk(ObjectNode obj, StringSubstitutor subs) {
    obj.properties()
        .forEach(
            (entry) -> {
              switch (entry.getValue()) {
                case TextNode text -> {
                  obj.put(entry.getKey(), subs.replace(text.textValue()));
                }
                case ObjectNode nested -> {
                  walk(nested, subs);
                }
                case ArrayNode arr -> {
                  walk(arr, subs);
                }
                default -> {}
              }
            });
  }

  private static void walk(ArrayNode arr, StringSubstitutor subs) {
    for (int i = 0; i < arr.size(); i++) {
      var child = arr.get(i);

      switch (child) {
        case TextNode text -> {
          String value = subs.replace(text.textValue());
          arr.set(i, TextNode.valueOf(value));
        }
        case ObjectNode nested -> {
          walk(nested, subs);
        }
        case ArrayNode nestedArr -> {
          walk(nestedArr, subs);
        }
        default -> {}
      }
    }
  }
}
