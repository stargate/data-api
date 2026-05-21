package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.versioning.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.versioning.SchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.versioning.VersionedSchema;

/**
 * Factory for creating the {@link CollectionRerankDef} as a schema value, access via the {@link
 * VersionedSchema}
 */
public class CollectionRerankDefSchemaFactory extends SchemaFactory<CollectionRerankDef> {

  private static final CollectionRerankDef FOR_TESTING_DEFAULT =
      new CollectionRerankDef(
          true,
          new CollectionRerankDef.RerankServiceDef("nvidia", "nvidia/llama-3.2-nv-rerankqa-1b-v2", null, null));

  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionRerankDefSchemaFactory(
          CollectionSchemaVersion.V_2,
          CollectionRerankDef.configForPreRerankingCollection(),
          CollectionSchemaVersion.V_2,
          FOR_TESTING_DEFAULT,
          false,
          CollectionRerankDef.configForDisabled());

  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionRerankDefSchemaFactory(
          CollectionSchemaVersion.V_2,
          CollectionRerankDef.configForPreRerankingCollection(),
          CollectionSchemaVersion.V_2,
          FOR_TESTING_DEFAULT,
          true,
          CollectionRerankDef.configForDisabled());

  public CollectionRerankDefSchemaFactory(boolean featureDisabled) {
    this(
        CollectionSchemaVersion.V_2,
        CollectionRerankDef.configForPreRerankingCollection(),
        CollectionSchemaVersion.V_2,
        CollectionRerankDef.configForDefault(),
        featureDisabled,
        CollectionRerankDef.configForDisabled());
  }

  private CollectionRerankDefSchemaFactory(
      CollectionSchemaVersion releasedVersion,
      CollectionRerankDef preReleaseValue,
      CollectionSchemaVersion currentVersion,
      CollectionRerankDef currentDefault,
      boolean featureDisabled,
      CollectionRerankDef featureDisabledDefault) {
    super(
        CollectionRerankDef.class,
        releasedVersion,
        preReleaseValue,
        currentVersion,
        currentDefault,
        featureDisabled,
        featureDisabledDefault);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      CollectionSchemaVersion candidateVersion, CollectionRerankDef candidatePersisted) {
    throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
  }
}
