package io.stargate.sgv2.jsonapi.service.schema.versioning;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;

public class RerankDefSchemaValueDef extends SchemaValueDef<CollectionRerankDef> {

  @VisibleForTesting
  public static final RerankDefSchemaValueDef FOR_TESTING_ENABLED =
      new RerankDefSchemaValueDef(false);

  @VisibleForTesting
  public static final RerankDefSchemaValueDef FOR_TESTING_DISABLED =
      new RerankDefSchemaValueDef(true);

  RerankDefSchemaValueDef(boolean featureDisabled) {
    super(
        CollectionRerankDef.class,
        SchemaVersion.V_2,
        CollectionRerankDef.configForPreRerankingCollection(),
        SchemaVersion.V_2,
        CollectionRerankDef.configForDefault(),
        featureDisabled,
        CollectionRerankDef.configForDisabled());
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      SchemaVersion candidateVersion, CollectionRerankDef candidatePersisted) {
    throw SchemaException.Code.RERANKING_FEATURE_NOT_ENABLED.get();
  }
}
