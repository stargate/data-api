package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Class similar to {@link DocumentProjector} but used for Table API, for non-nested
 * inclusion/exclusion projections.
 */
public class TableProjector {
  private static final TableProjector INCLUDE_ALL_PROJECTOR =
      new TableProjector(false, Collections.emptyList());

  private static final TableProjector EXCLUDE_ALL_PROJECTOR =
      new TableProjector(true, Collections.emptyList());

  private final boolean inclusion;

  private final List<String> columnNames;

  private TableProjector(boolean inclusion, List<String> columnNames) {
    this.inclusion = inclusion;
    this.columnNames = columnNames;
  }

  public static TableProjector createFromDefinition(JsonNode projectionDefinition) {
    // First special case: "simple" default projection; "include all"
    if (projectionDefinition == null || projectionDefinition.isEmpty()) {
      return INCLUDE_ALL_PROJECTOR;
    }
    if (!projectionDefinition.isObject()) {
      throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
          "definition must be OBJECT, was %s", projectionDefinition.getNodeType());
    }
    // Special cases: "star-include/exclude"
    if (projectionDefinition.size() == 1) {
      Map.Entry<String, JsonNode> entry = projectionDefinition.fields().next();
      if ("*".equals(entry.getKey())) {
        boolean includeAll = extractIncludeOrExclude(entry.getKey(), entry.getValue());
        if (includeAll) {
          return INCLUDE_ALL_PROJECTOR;
        }
        return EXCLUDE_ALL_PROJECTOR;
      }
    }
    return createFromNonEmpty(projectionDefinition);
  }

  private static TableProjector createFromNonEmpty(JsonNode projectionDefinition) {
    List<String> columnNames = new ArrayList<>();
    boolean inclusionProjection = false;

    var it = projectionDefinition.fields();
    while (it.hasNext()) {
      var entry = it.next();
      String path = entry.getKey();

      if (path.isEmpty()) {
        throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "empty paths (and path segments) not allowed");
      }

      // Special rule for "*": only allowed as single root-level entry;
      if ("*".equals(path)) {
        throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
            "wildcard ('*') only allowed as the only root-level path");
      }
      JsonNode value = entry.getValue();
      boolean addInclusion = extractIncludeOrExclude(path, value);

      // If the first entry, we know inclusion/exclusion; if other, need to ensure
      // there's no mixing of inclusion/exclusion
      if (columnNames.isEmpty()) {
        inclusionProjection = addInclusion;
      } else if (inclusionProjection != addInclusion) {
        if (addInclusion) {
          throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
              "cannot include '%s' on exclusion projection", path);
        } else {
          throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
              "cannot exclude '%s' on inclusion projection", path);
        }
      }
      columnNames.add(path);
    }
    return new TableProjector(inclusionProjection, columnNames);
  }

  /**
   * Method that uses this Projection to filter matching columns from a map of column definitions.
   *
   * @param columnDefs Column definitions by matching name to proper identifier
   * @return Filtered List of matching columns
   * @param <T> Actual column identifier type
   */
  public <T> List<T> filterColumns(Map<String, T> columnDefs) {
    // "missing" root layer used as short-cut for include-all/exclude-all
    if (columnNames.isEmpty()) {
      if (inclusion) { // exclude-all
        return Collections.emptyList();
      }
      // include-all
      return columnDefs.values().stream().toList();
    }

    // Otherwise need to actually determine
    if (inclusion) {
    } else {
    }
    return columnDefs.values().stream().toList();
  }

  private static boolean extractIncludeOrExclude(String path, JsonNode value) {
    if (value.isNumber()) {
      // "0" means exclude (like false); any other number include
      return !BigDecimal.ZERO.equals(value.decimalValue());
    }
    if (value.isBoolean()) {
      return value.booleanValue();
    }
    if (value.isObject()) {
      throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
          "path ('%s') value cannot be OBJECT: nesting not supported for Tables", path);
    }

    // Unknown JSON node type; error
    throw ErrorCode.UNSUPPORTED_PROJECTION_PARAM.toApiException(
        "path ('%s') value must be NUMBER or BOOLEAN, was %s", path, value.getNodeType());
  }
}
