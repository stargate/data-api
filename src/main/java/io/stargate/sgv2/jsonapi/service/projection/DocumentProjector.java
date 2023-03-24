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
  /** Pseudo-projector that makes no modifications to documents */
  private static final DocumentProjector IDENTITY_PROJECTOR = new DocumentProjector(null, true);

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
    if (rootLayer != null) { // null -> identity projection (no-op)
      throw new JsonApiException(
          ErrorCode.UNSUPPORTED_PROJECTION_PARAM, "Non-identity Projections not yet supported");
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

    private int exclusions, inclusions;

    private Boolean idInclusion = null;

    private PathCollector() {}

    static PathCollector collectPaths(JsonNode def) {
      return new PathCollector().collectFromObject(def, null);
    }

    public DocumentProjector buildProjector() {
      if (isIdentityProjection()) {
        return DocumentProjector.identityProjector();
      }

      // One more thing: do we need to add document id?
      if (inclusions > 0) { // inclusion-based projection
        // doc-id included unless explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, !Boolean.FALSE.equals(idInclusion)), true);
      } else { // exclusion-based
        // doc-id excluded only if explicitly excluded
        return new DocumentProjector(
            ProjectionLayer.buildLayers(paths, Boolean.FALSE.equals(idInclusion)), false);
      }
    }

    /**
     * Accessor to use for checking if collected paths indicate "empty" (no-operation) projection:
     * if so, caller can avoid actual construction or evaluation.
     */
    boolean isIdentityProjection() {
      // Only the case if we have no non-doc-id inclusions/exclusions AND
      // doc-id is included (by default or explicitly)
      return paths.isEmpty() && !Boolean.FALSE.equals(idInclusion);
    }

    PathCollector collectFromObject(JsonNode ob, String parentPath) {
      var it = ob.fields();
      while (it.hasNext()) {
        var entry = it.next();
        String path = entry.getKey();
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
