package io.stargate.sgv2.jsonapi.util.recordable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collection;
import java.util.Objects;

/**
 * A recorder that will convert a Recordable object into a JsonNode.
 *
 * <p>Call {@link #toJson(Recordable)} to convert the Recordable object into a JsonNode.
 */
public interface Jsonable {

  /**
   * Recursively converts a Recordable object into a JsonNode.
   *
   * @param recordable The Recordable object to convert.
   * @return The JsonNode representation of the Recordable object.
   */
  static JsonNode toJson(Recordable recordable) {
    Objects.requireNonNull(recordable, "recordable must not be null");

    try {
      return ((JsonsableRecorder) recordable.recordTo(new JsonsableRecorder(recordable.getClass())))
          .toJsonNode();
    } catch (RuntimeException e) {
      // Safety to now let exceptions from the Recordable object escape, as they are not expected
      var errorNode = JsonNodeFactory.instance.objectNode();
      errorNode.put("error", "Failed to convert Recordable to JsonNode");
      errorNode.put("recordable.getClass()", recordable.getClass().getName());
      errorNode.put("exception", e.toString());
      return errorNode;
    }
  }

  class JsonsableRecorder extends Recordable.DataRecorder {

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

    // The root container node, either an array or object
    // Only set for the root recorder
    private JsonNode rootNode;
    // The current node append will add to.
    private final JsonNode jsonNode;

    // The last key added to the object, when the value added is recordable it will
    // result in a sub recorder created, which needs this key to know where to add the object
    private String lastKey = null;

    // If we are building an array of values, this is the array the new sub recorder needs to add to
    private ArrayNode parentArray = null;

    /**
     * Creates a new {@link JsonsableRecorder} for the given class.
     *
     * @param clazz The class to record.
     */
    public JsonsableRecorder(Class<?> clazz) {
      this(clazz, null);
    }

    private JsonsableRecorder(Class<?> clazz, JsonsableRecorder parent) {
      super(clazz, parent);

      if (parent == null) {
        // this is the root recorder
        if (Recordable.RecordableCollection.class.isAssignableFrom(clazz)) {
          // we have a collection of recordable objects, so we want the root JSON object to be
          // an array
          jsonNode = JSON_NODE_FACTORY.arrayNode();
          rootNode = jsonNode;
        } else if (Recordable.class.isAssignableFrom(clazz)) {
          // root object is Recordable, so we want the root JSON object to be an object
          var rootObject = JSON_NODE_FACTORY.objectNode();
          rootNode = rootObject;
          // the data is added under the class name
          jsonNode = rootObject.putObject(className(clazz));
        } else {
          // Sanity check
          throw new IllegalArgumentException(
              "Class must be a Recordable or a RecordableCollection, got " + clazz.getName());
        }
        return;
      }

      // This is a sub object, there is a parent
      if (parent.parentArray != null) {
        // we are adding the sub object in an array
        // and we still put the object under the class name
        jsonNode = parent.parentArray.addObject().putObject(className(clazz));

      } else if (parent.lastKey != null) {
        // Sub object is added to the parent object under the last key, e.g.
        // the parent has a "foo" value that was recordable
        if (parent.jsonNode instanceof ObjectNode on) {
          jsonNode = on.putObject(parent.lastKey).putObject(className(clazz));
        } else {
          throw new IllegalArgumentException("Cannot add object to a non-object node");
        }
      } else {
        if (parent.jsonNode instanceof ObjectNode on) {
          jsonNode = on.putObject(className(clazz));
        } else {
          throw new IllegalArgumentException("Cannot add object to a non-object node");
        }
      }
    }

    public JsonNode toJsonNode() {
      return rootNode != null ? rootNode : jsonNode;
    }

    @Override
    public Recordable.DataRecorder beginSubRecorder(Class<?> clazz) {
      return new JsonsableRecorder(clazz, this);
    }

    @Override
    public Recordable.DataRecorder endSubRecorder() {
      return parent;
    }

    @Override
    public Recordable.DataRecorder append(String key, Object value) {
      lastKey = key;

      ArrayNode thisArray = jsonNode instanceof ArrayNode an ? an : null;
      parentArray = thisArray;

      ObjectNode thisObject = jsonNode instanceof ObjectNode on ? on : null;

      // Order of the switch is important, check recordable before list etc
      switch (value) {
        case null -> {
          if (thisArray != null) {
            thisArray.addNull();
          } else if (thisObject != null) {
            thisObject.putNull(key);
          }
        }
        case Recordable recordable -> recordable.recordToSubRecorder(this);
        case Collection<?> list -> {
          var arrayNode = thisObject == null ? thisArray : thisObject.putArray(key);
          parentArray = arrayNode;
          for (Object item : list) {
            switch (item) {
              case null -> arrayNode.addNull();
              case Recordable recordable -> recordable.recordToSubRecorder(this);
              case Number number -> arrayNode.add(number.doubleValue());
              case Boolean bool -> arrayNode.add(bool);
              case String string -> arrayNode.add(string);
              default -> arrayNode.add(Objects.toString(item));
            }
          }
        }
        case Number number -> {
          if (thisArray != null) {
            thisArray.add(number.doubleValue());
          } else if (thisObject != null) {
            thisObject.put(key, number.doubleValue());
          }
        }
        case Boolean bool -> {
          if (thisArray != null) {
            thisArray.add(bool);
          } else if (thisObject != null) {
            thisObject.put(key, bool);
          }
        }
        case String string -> {
          if (thisArray != null) {
            thisArray.add(string);
          } else if (thisObject != null) {
            thisObject.put(key, string);
          }
        }
        default -> {
          if (thisArray != null) {
            thisArray.add(Objects.toString(value));
          } else if (thisObject != null) {
            thisObject.put(key, Objects.toString(value));
          }
        }
      }

      parentArray = null;
      return this;
    }
  }
}
