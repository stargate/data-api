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

  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionRerankDefSchemaFactory(false);

  @VisibleForTesting
  public static final CollectionRerankDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionRerankDefSchemaFactory(true);

  public CollectionRerankDefSchemaFactory(boolean featureDisabled) {
    super(
        CollectionRerankDef.class,
        CollectionSchemaVersion.V_2,
        CollectionRerankDef.configForPreRerankingCollection(),
        CollectionSchemaVersion.V_2,
        CollectionRerankDef.configForDefault(),
        featureDisabled,
        CollectionRerankDef.configForDisabled());
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      CollectionSchemaVersion candidateVersion, CollectionRerankDef candidatePersisted) {
    throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
  }
}
