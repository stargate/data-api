package io.stargate.sgv2.jsonapi.api.model.command.table;

import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Enum for API map component representation. It could be used in createIndex for map column,
 * filtering on map column.
 */
public enum ApiMapComponent {
  KEYS(TableDescConstants.MapTypeComponent.keys),
  VALUES(TableDescConstants.MapTypeComponent.values);

  private final String apiName;

  private static final Map<String, ApiMapComponent> COMPONENT_MAP;

  static {
    COMPONENT_MAP = new HashMap<>();
    for (ApiMapComponent component : ApiMapComponent.values()) {
      COMPONENT_MAP.put(component.apiName.toLowerCase(), component);
    }
  }

  public static Optional<ApiMapComponent> fromApiName(String userInput) {
    return Optional.ofNullable(userInput).map(String::toLowerCase).map(COMPONENT_MAP::get);
  }

  ApiMapComponent(String value) {
    this.apiName = value;
  }

  public String getApiName() {
    return apiName;
  }
}
