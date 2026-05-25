package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.versioning.*;

/**
 * Factory for creating the {@link CollectionRerankDef} as a schema value, access via the {@link
 * VersionedSchema}
 */
public class CollectionRerankDefSchemaFactory extends CollectionSchemaFactory<CollectionRerankDef> {

  // FOR TESTING ONLY - the default for CollectionRerankDef is built at run time from config
  // this is a hack so we have a stable default for testing that does not depend on the injected
  // config.
  private static final CollectionRerankDef FOR_TESTING_DEFAULT =
      new CollectionRerankDef(
          true,
          new CollectionRerankDef.RerankServiceDef(
              "nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null));

  private static final SchemaDefaults<CollectionRerankDef> FOR_TESTING_DEFAULTS =
      new SchemaDefaults<>() {
        @Override
        public CollectionRerankDef forPreRelease() {
          return CollectionRerankDef.SCHEMA_DEFAULTS.forPreRelease();
        }

        @Override
        public CollectionRerankDef currentDefault() {
          return FOR_TESTING_DEFAULT;
        }

        @Override
        public CollectionRerankDef forDisabledFeature() {
          return CollectionRerankDef.SCHEMA_DEFAULTS.forDisabledFeature();
        }
      };

  /** Use this only for testing, it ignores the {@link ApiFeatures} config. */
  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionRerankDefSchemaFactory(
          FOR_TESTING_DEFAULTS, CollectionSchemaVersion.V_2, CollectionSchemaVersion.V_2, false);

  /** Use this only for testing, it ignores the {@link ApiFeatures} config. */
  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionRerankDefSchemaFactory(
          FOR_TESTING_DEFAULTS, CollectionSchemaVersion.V_2, CollectionSchemaVersion.V_2, true);

  public CollectionRerankDefSchemaFactory(boolean featureDisabled) {
    this(
        CollectionRerankDef.SCHEMA_DEFAULTS,
        CollectionSchemaVersion.V_2,
        CollectionSchemaVersion.V_2,
        featureDisabled);
  }

  private CollectionRerankDefSchemaFactory(
      SchemaDefaults<CollectionRerankDef> schemaDefaults,
      CollectionSchemaVersion releasedVersion,
      CollectionSchemaVersion currentVersion,
      boolean featureDisabled) {
    super(
        CollectionRerankDef.class,
        schemaDefaults,
        releasedVersion,
        currentVersion,
        featureDisabled);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      SchemaVersion candidateVersion, CollectionRerankDef candidatePersisted) {
    throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
  }
}
