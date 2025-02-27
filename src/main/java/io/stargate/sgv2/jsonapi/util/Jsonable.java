package io.stargate.sgv2.jsonapi.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Objects;

public interface Jsonable {

  static JsonNode toJson(Recordable recordable) {
    return ((JsonsableRecorder) recordable.recordTo(new JsonsableRecorder(recordable.getClass())))
        .toJsonNode();
  }

  class JsonsableRecorder extends Recordable.DataRecorder {

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.instance;

    private final ObjectNode objectNode;

    private String lastKey = null;

    public JsonsableRecorder(Class<?> clazz) {
      this(clazz, null);
    }

    private JsonsableRecorder(Class<?> clazz, JsonsableRecorder parent) {
      super(clazz, false, parent);

      ObjectNode parentObjectNode = null;

      if (parent == null){
        parentObjectNode = JSON_NODE_FACTORY.objectNode();
      } else if (parent.lastKey != null) {
        parentObjectNode = parent.objectNode.putObject(parent.lastKey);
      }
      else {
        parentObjectNode = parent.objectNode;
      }
      this.objectNode = parentObjectNode.putObject(className(clazz));
    }

    public JsonNode toJsonNode() {
      return objectNode;
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
      switch (value) {
        case null -> objectNode.putNull(key);
        case Recordable recordable -> recordable.recordToSubRecorder(this);
        case List<?> list -> {
          var arrayNode = objectNode.putArray(key);
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
        case Number number -> objectNode.put(key, number.doubleValue());
        case Boolean bool -> objectNode.put(key, bool);
        case String string -> objectNode.put(key, string);
        default -> objectNode.put(key, Objects.toString(value));
      }
      return this;
    }
  }
}
