package io.stargate.sgv2.jsonapi.api.model.command.table;

/**
 * Enum representing the components of a collection filter. This is used to specify which part of a
 * collection (list, set, or map) is being filtered in a query.
 *
 * <p>Note: This enum is only used internally, refer to {@link ApiMapComponent} for API map
 * component definition.
 */
public enum MapSetListComponent {
  LIST_VALUE,
  SET_VALUE,
  MAP_ENTRY,
  MAP_KEY,
  MAP_VALUE;

  /** Converts the given {@link ApiMapComponent} to a {@link MapSetListComponent}. */
  public static MapSetListComponent fromMapComponent(ApiMapComponent apiMapComponent) {
    return switch (apiMapComponent) {
      case KEYS -> MAP_KEY;
      case VALUES -> MAP_VALUE;
    };
  }
}
