package io.stargate.sgv2.jsonapi.api.model.command.table;

import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum for API map component representation. It could be used in createIndex for map column,
 * filtering on map column.
 */
public enum ApiMapComponent {
  KEYS(TableDescConstants.MapTypeComponent.keys),
  VALUES(TableDescConstants.MapTypeComponent.values);

  private final String value;

  private static final Map<String, ApiMapComponent> COMPONENT_MAP;

  static {
    COMPONENT_MAP = new HashMap<>();
    for (ApiMapComponent component : ApiMapComponent.values()) {
      COMPONENT_MAP.put(component.value.toLowerCase(), component);
    }
  }

  public static ApiMapComponent fromUserInput(String userInput) {
    if (userInput == null) {
      throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
    }
    var lowerCaseInput = userInput.toLowerCase();
    if (!COMPONENT_MAP.containsKey(lowerCaseInput)) {
      throw SchemaException.Code.INVALID_FORMAT_FOR_INDEX_CREATION_COLUMN.get();
    }
    return COMPONENT_MAP.get(lowerCaseInput);
  }

  ApiMapComponent(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
