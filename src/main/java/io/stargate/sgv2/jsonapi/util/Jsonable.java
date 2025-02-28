package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Collection;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface Jsonable {
  static final Logger LOGGER = LoggerFactory.getLogger(Jsonable.class);

  static JsonNode toJson(Recordable recordable) {
    return ((JsonsableRecorder) recordable.recordTo(new JsonsableRecorder(recordable.getClass())))
        .toJsonNode();
  }

  class JsonsableRecorder extends Recordable.DataRecorder {

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

    private final JsonNode jsonNode;

    private String lastKey = null;
    private ArrayNode parentArray = null;

    public JsonsableRecorder(Class<?> clazz) {
      this(clazz, null);
    }

    private JsonsableRecorder(Class<?> clazz, JsonsableRecorder parent) {
      super(clazz, false, parent);

      if (parent == null) {
        if (Recordable.Array.class.isAssignableFrom(clazz)) {
          // if  we are starting with a list / collection then we will start with an array node
          jsonNode = JSON_NODE_FACTORY.arrayNode();
        } else if (Recordable.class.isAssignableFrom(clazz)) {
          jsonNode = JSON_NODE_FACTORY.objectNode().putObject(className(clazz));
        } else {
          throw new IllegalArgumentException(
              "Class must be a Recordable or a Collection, got " + clazz.getName());
        }

      } else if (parent.parentArray != null) {

        jsonNode = parent.parentArray.addObject().putObject(className(clazz));

      } else if (parent.lastKey != null) {

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
      return jsonNode;
    }

    @Override
    public Recordable.DataRecorder beginSubRecorder(Class<?> clazz) {
      return new JsonsableRecorder(clazz, this);
    }

    @Override
    public Recordable.DataRecorder endSubRecorder() {
      //      if (parent != null) {
      //        indent();
      //      }
      //      sb.append("}");
      //      newLine();
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
