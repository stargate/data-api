package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;

import java.util.List;
import java.util.UUID;
import org.apache.commons.text.StringSubstitutor;

public record TestRequest(@JsonIgnore UUID requestId, ObjectNode request) {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public TestRequest(ObjectNode request) {
    this(UUID.randomUUID(), request);
  }

  public CommandName commandName() {
    var requestCommandName = commandNameString();
    for (CommandName name : CommandName.values()) {
      if (name.getApiName().equals(requestCommandName)) {
        return name;
      }
    }
    throw new IllegalArgumentException("Unknown command name: " + requestCommandName);
  }

  private String commandNameString() {
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

  public ObjectNode withEnvironment(IntegrationEnv env) {
    ObjectNode updated = request.deepCopy();
    walk(updated, env.substitutor());
    return updated;
  }

  public static TestRequest fromJson(String json) {

    try {
      return  new TestRequest((ObjectNode) OBJECT_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

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
