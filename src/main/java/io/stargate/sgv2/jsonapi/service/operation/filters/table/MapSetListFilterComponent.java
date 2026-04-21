package io.stargate.sgv2.jsonapi.service.operation.filters.table;

import io.stargate.sgv2.jsonapi.api.model.command.table.definition.datatype.MapComponentDesc;

/** The map/set/list component that will be filtered against. */
public enum MapSetListFilterComponent {
  LIST_VALUE,
  SET_VALUE,
  MAP_ENTRY,
  MAP_KEY,
  MAP_VALUE;

  public static MapSetListFilterComponent fromMapComponentDesc(MapComponentDesc mapComponentDesc) {
    return switch (mapComponentDesc) {
      case KEYS -> MAP_KEY;
      case VALUES -> MAP_VALUE;
        // There is no $entries for map, null will be default to entries
      case null -> MAP_ENTRY;
    };
  }
}
