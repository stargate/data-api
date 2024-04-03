package io.stargate.sgv2.jsonapi.service.projection;

import com.fasterxml.jackson.databind.JsonNode;
import io.stargate.sgv2.jsonapi.config.constants.DocumentConstants;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Alternative to {@link ProjectionLayer} that is used for indexing (projection) purposes, to
 * support "deny"/"allow" rules for defining properties to index (or not)
 */
public class IndexingProjector {
  /**
   * No-op projector that does not modify documents. Considered "exclusion" projector since "no
   * exclusions" is conceptually what happens ("no inclusions" would drop all content)
   */
  private static final IndexingProjector IDENTITY_PROJECTOR =
      new IndexingProjector(null, false, false);

  private final ProjectionLayer rootLayer;

  /** Whether this projector is inclusion- ({@code true}) or exclusion ({@code false}) based. */
  private final boolean inclusion;

  /** An override flag set when indexing option is deny all */
  private final boolean indexingDenyAll;

  private IndexingProjector(ProjectionLayer rootLayer, boolean inclusion, boolean indexingDenyAll) {
    this.rootLayer = rootLayer;
    this.inclusion = inclusion;
    this.indexingDenyAll = indexingDenyAll;
  }

  public boolean isIndexingDenyAll() {
    return indexingDenyAll;
  }

  public static IndexingProjector createForIndexing(Set<String> allowed, Set<String> denied) {
    // Sets are expected to be validated to have one of 3 main cases:
    // 1. Non-empty "allowed" (but empty/null "denied") -> build inclusion projection
    // 2. Non-empty "denied" (but empty/null "allowed") -> build exclusion projection
    // 3. Empty/null "allowed" and "denied" -> return identity projection
    // as well as 2 special cases:
    // 4. Empty "allowed" and single "*" entry for "denied" -> return exclude-all projection
    // 5. Empty "deny" and single "*" entry for "allowed" -> return include-all ("identity")
    // projection
    // We need not (and should not) do further validation here.
    // Note that (5) is effectively same as (3) and included for sake of uniformity
    if (allowed != null && !allowed.isEmpty()) {
      // (special) Case 5:
      if (allowed.size() == 1 && allowed.contains("*")) {
        return identityProjector();
      }
      // Case 1: inclusion-based projection
      // Minor complication: "$vector" needs to be included automatically
      if (!allowed.contains(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD)) {
        allowed.add(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
      }
      if (!allowed.contains(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD)) {
        allowed.add(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
      }
      return new IndexingProjector(ProjectionLayer.buildLayersOverlapOk(allowed), true, false);
    }
    if (denied != null && !denied.isEmpty()) {
      // (special) Case 4:
      if (denied.size() == 1 && denied.contains("*")) {
        // Basically inclusion projector with nothing to include but handle for $vector and
        // $vectorize
        Set<String> overrideFields = new HashSet<>();
        overrideFields.add(DocumentConstants.Fields.VECTOR_EMBEDDING_FIELD);
        overrideFields.add(DocumentConstants.Fields.VECTOR_EMBEDDING_TEXT_FIELD);
        return new IndexingProjector(
            ProjectionLayer.buildLayersOverlapOk(overrideFields), true, true);
      }
      // Case 2: exclusion-based projection
      return new IndexingProjector(ProjectionLayer.buildLayersOverlapOk(denied), false, false);
    }
    // Case 3: include-all (identity) projection
    return identityProjector();
  }

  public static IndexingProjector identityProjector() {
    return IDENTITY_PROJECTOR;
  }

  public boolean isIdentityProjection() {
    return rootLayer == null && !inclusion;
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

  /**
   * Method to call to check if given path (dotted path, that is, dot-separated segments) would be
   * included by this Projection. That is, either
   *
   * <ul>
   *   <li>This is inclusion projection, and path is covered by an inclusion path
   *   <li>This is exclusion projection, and path is NOT covered by any exclusion path
   * </ul>
   *
   * @param path Dotted path (possibly nested) to check
   * @return {@code true} if path is included; {@code false} if not.
   */
  public boolean isPathIncluded(String path) {
    // First: if we have no layers, we are identity projector and include everything
    if (rootLayer == null) {
      return true;
    }
    // Otherwise need to split path, evaluate; but note reversal wrt include/exclude
    // projections
    if (inclusion) {
      return rootLayer.isPathIncluded(path);
    }
    return !rootLayer.isPathIncluded(path);
  }

  // Mostly for deserialization tests
  @Override
  public boolean equals(Object o) {
    if (o instanceof IndexingProjector) {
      IndexingProjector other = (IndexingProjector) o;
      return (this.inclusion == other.inclusion)
          && (this.indexingDenyAll == other.indexingDenyAll)
          && Objects.equals(this.rootLayer, other.rootLayer);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return rootLayer.hashCode();
  }
}
