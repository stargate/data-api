package io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype;

import io.stargate.sgv2.jsonapi.config.constants.TableDescConstants;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The component of a CQL Map Column that is referenced in a filter or update operation.
 *
 * <p>This is the public API representation of the map components, see also {@link
 * io.stargate.sgv2.jsonapi.service.operation.filters.table.MapSetListFilterComponent}
 */
public enum MapComponentDesc {
  KEYS(TableDescConstants.MapTypeComponent.keys),
  VALUES(TableDescConstants.MapTypeComponent.values);

  private final String apiName;

  private static final Map<String, MapComponentDesc> COMPONENT_MAP;

  static {
    COMPONENT_MAP = new HashMap<>();
    for (MapComponentDesc component : MapComponentDesc.values()) {
      COMPONENT_MAP.put(component.apiName.toLowerCase(), component);
    }
  }

  public static Optional<MapComponentDesc> fromApiName(String userInput) {
    return userInput == null
        ? Optional.empty()
        : Optional.ofNullable(COMPONENT_MAP.get(userInput.toLowerCase()));
  }

  MapComponentDesc(String value) {
    this.apiName = value;
  }

  public String getApiName() {
    return apiName;
  }
}
