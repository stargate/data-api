package io.stargate.sgv2.jsonapi.testbench.testspec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.stargate.sgv2.jsonapi.api.model.command.CommandName;
import io.stargate.sgv2.jsonapi.testbench.testrun.TestRunEnv;
import org.apache.commons.text.StringSubstitutor;

/**
 * A API command to send, that may be part of a setup, test, cleanup, or lifecycle process. This is a
 * basic command to send, without any checks etc.
 * <p>
 * This class is re-used in Spec configurations where an API command is needed. For example below, the TestCommand
 * is the Objects in the setup array. The structure is then a normal Data API command object, with a single top level
 * member that is the name of the command.
 * <pre>
 *   "setup": [
 *     {
 *       "insertOne": {
 *         "document": {
 *           "_id": "Inception",
 *           "name": "Inception",
 *           "genre": "Science Fiction",
 *           "artist": [
 *             "Leonardo DiCaprio"
 *           ],
 *           "$vectorize": "Inception is a science fiction action film about a professional thief who steals information by infiltrating the subconscious, entering people's dreams. He is offered a chance to have his criminal history erased as payment for implanting another person's idea into a target's subconscious."
 *         }
 *       }
 *     },
 *     {
 *       "insertMany": {
 *         "documents": [....
 * </pre>
 * </p>
 */
public class TestCommand {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ObjectNode request;
  private final CommandName commandName;
  private final String includeFrom;

  /**
   * Constructor called when this class is used in a record etc that is deserialized using Jackson.
   * @param request The JSON object to deserialize from, e.g. the objects in the array above.
   */
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  public TestCommand(ObjectNode request) {

    // if non-null, that this is a pointer to find commands in the named test-suite.
    var includeField = request.get("$include");
    this.includeFrom = includeField == null ? null : includeField.asText();

    if (includeField != null) {
      this.request = null;
      this.commandName = null;
    } else {
      this.request = request;
      this.commandName = commandName(request);
    }
  }

  /**
   * Get the complete request to send
   */
  public ObjectNode request() {
    throwIfHasInclude();
    return request;
  }

  /**
   * The name of the Data API command, extracted from the definition.
   */
  public CommandName commandName() {
    throwIfHasInclude();
    return commandName;
  }

  /**
   * Name of the test-suite to include command from, rather than use the definition in here.
   * <p>
   * For example, this says to include setup commands from 'vectorize-base'
   *
   * <pre>
   *   "setup": [
   *     {
   *       "$include": "vectorize-base"
   *     }
   *   ],
   * </pre>
   * </p>
   * @return value of the '$include' from the json
   */
  public String includeFrom() {
    return includeFrom;
  }

  /**
   * Build from a string, useful for lifecycle commands that are created on the fly.
   */
  public static TestCommand fromJson(String json) {

    try {
      return new TestCommand((ObjectNode) OBJECT_MAPPER.readTree(json));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Extracts the name of the API command from the request.
   * <p>
   *  Public for re-use.
   * </p>
   */
  public static CommandName commandName(ObjectNode request) {
    var requestCommandName = commandNameString(request);
    for (CommandName name : CommandName.values()) {
      if (name.getApiName().equals(requestCommandName)) {
        return name;
      }
    }
    throw new IllegalArgumentException("Unknown command name: " + requestCommandName);
  }

  public ObjectNode withEnvironment(TestRunEnv env) {
    ObjectNode updated = request.deepCopy();
    walk(updated, env.substitutor());
    return updated;
  }

  private void throwIfHasInclude() {
    if (includeFrom != null) {
      throw new IllegalStateException("TestCommand is defined to $include from: " + includeFrom);
    }
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
