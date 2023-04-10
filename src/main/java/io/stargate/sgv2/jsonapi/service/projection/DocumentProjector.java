package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import io.stargate.sgv2.jsonapi.exception.ErrorCode;
import io.stargate.sgv2.jsonapi.exception.JsonApiException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Helper class that implements functionality needed to support projections on documents fetched via
 * various {@code find} commands.
 */
public class DocumentProjector {
  /**
   * No-op projector that does not modify documents. Considered "exclusion" projector since "no
   * exclusions" is conceptually what happens ("no inclusions" would drop all content)
   */
  private static final DocumentProjector IDENTITY_PROJECTOR = new DocumentProjector(null, false);

  private final ProjectionLayer rootLayer;

  /** Whether this projector is inclusion- ({@code true}) or exclusion ({@code false}) based. */
  private final boolean inclusion;

  private DocumentProjector(ProjectionLayer rootLayer, boolean inclusion) {
    this.rootLayer = rootLayer;
    this.inclusion = inclusion;
  }

  public static DocumentProjector createFromDefinition(JsonNode projectionDefinition) {
    if (projectionDefinition == null) {
      return identityProjector();
    }
    if (!projectionDefinition.isObject()) {
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": definition must be OBJECT, was "
              + projectionDefinition.getNodeType());
    }
    return PathCollector.collectPaths(projectionDefinition).buildProjector();
  }

  public static DocumentProjector identityProjector() {
    return IDENTITY_PROJECTOR;
  }

  public boolean isInclusion() {
    return inclusion;
  }

  public void applyProjection(JsonNode document) {
    if (rootLayer == null) { // null -> identity projection (no-op)
      return;
    }

    if (inclusion) {
      rootLayer.applyInclusions(document);
    } else {
      rootLayer.applyExclusions(document);
    }
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof DocumentProjector) {
      DocumentProjector other = (DocumentProjector) o;
      return (this.inclusion == other.inclusion) && Objects.equals(this.rootLayer, other.rootLayer);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return rootLayer.hashCode();
  }

  /**
   * Helper object used to traverse and collection inclusion/exclusion path definitions and verify
   * that there are only one or the other (except for doc id). Does not build data structures for
   * actual matching.
   */
  private static class PathCollector {
    private List<String> paths = new ArrayList<>();

    private List<ProjectionLayer.SliceDef> slices = new ArrayList<>();

    private int exclusions, inclusions;

    private Boolean idInclusion = null;

    private PathCollector() {}

    static PathCollector collectPaths(JsonNode def) {
      return new PathCollector().collectFromObject(def, null);
    }

    public DocumentProjector buildProjector() {
      if (isIdentityProjection()) {
        return identityProjector();
      }

      // One more thing: do we need to add document id?
      if (inclusions > 0) { // inclusion-based projection
        // doc-id included unless explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, slices, !Boolean.FALSE.equals(idInclusion)), true);
      } else { // exclusion-based
        // doc-id excluded only if explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, slices, Boolean.FALSE.equals(idInclusion)), false);
      }
    }

    /**
     * Accessor to use for checking if collected paths indicate "empty" (no-operation) projection:
     * if so, caller can avoid actual construction or evaluation.
     */
    boolean isIdentityProjection() {
      // Only the case if we have no non-doc-id inclusions/exclusions AND
      // doc-id is included (by default or explicitly)
      return paths.isEmpty() && slices.isEmpty() && !Boolean.FALSE.equals(idInclusion);
    }

    PathCollector collectFromObject(JsonNode ob, String parentPath) {
      var it = ob.fields();
      while (it.hasNext()) {
        var entry = it.next();
        String path = entry.getKey();

        if (path.isEmpty()) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": empty paths (and path segments) not allowed");
        }
        if (path.charAt(0) == '$') {
          // First: no operators allowed at root level
          if (parentPath == null) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": path cannot start with '$' (no root-level operators)");
          }

          // Second: we only support one operator for now
          if (!"$slice".equals(path)) {
            throw new JsonApiException(
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
                ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                    + ": unrecognized/unsupported projection operator '"
                    + path
                    + "' (only '$slice' supported)");
          }

          addSlice(parentPath, entry.getValue());
          continue;
        }

        if (parentPath != null) {
          path = parentPath + "." + path;
        }
        JsonNode value = entry.getValue();
        if (value.isNumber()) {
          // "0" means exclude (like false); any other number include
          if (BigDecimal.ZERO.equals(value.decimalValue())) {
            addExclusion(path);
          } else {
            addInclusion(path);
          }
        } else if (value.isBoolean()) {
          if (value.booleanValue()) {
            addInclusion(path);
          } else {
            addExclusion(path);
          }
        } else if (value.isObject()) {
          // Nested definitions allowed, too
          collectFromObject(value, path);
        } else {
          // Unknown JSON node type; error
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": path ('"
                  + path
                  + "') value must be NUMBER, BOOLEAN or OBJECT, was "
                  + value.getNodeType());
        }
      }
      return this;
    }

    private void addSlice(String path, JsonNode sliceDef) {
      if (sliceDef.isArray()) {
        if (sliceDef.size() == 1 && sliceDef.get(0).isIntegralNumber()) {
          int count = sliceDef.get(0).intValue();
          slices.add(new ProjectionLayer.SliceDef(path, ProjectionLayer.constructSlicer(count)));
          return;
        }
        if (sliceDef.size() == 2
            && sliceDef.get(0).isIntegralNumber()
            && sliceDef.get(1).isIntegralNumber()) {
          int skip = sliceDef.get(0).intValue();
          int count = sliceDef.get(1).intValue();
          slices.add(
              new ProjectionLayer.SliceDef(path, ProjectionLayer.constructSlicer(skip, count)));
          return;
        }
      } else if (sliceDef.isIntegralNumber()) {
        int count = sliceDef.intValue();
        slices.add(new ProjectionLayer.SliceDef(path, ProjectionLayer.constructSlicer(count)));
        return;
      }
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
              + ": path ('"
              + path
              + "') has unsupported parameter for '$slice' ("
              + sliceDef.getNodeType()
              + "): only NUMBER or ARRAY with 1 or 2 NUMBERs accepted");
    }

    private void addExclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = false;
      } else {
        // Must not mix exclusions and inclusions
        if (inclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot exclude '"
                  + path
                  + "' on inclusion projection");
        }
        ++exclusions;
        paths.add(path);
      }
    }

    private void addInclusion(String path) {
      if (DocumentConstants.Fields.DOC_ID.equals(path)) {
        idInclusion = true;
      } else {
        // Must not mix exclusions and inclusions
        if (exclusions > 0) {
          throw new JsonApiException(
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM,
              ErrorCode.UNSUPPORTED_PROJECTION_PARAM.getMessage()
                  + ": cannot include '"
                  + path
                  + "' on exclusion projection");
        }
        ++inclusions;
        paths.add(path);
      }
    }
  }
}
