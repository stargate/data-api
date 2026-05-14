package io.stargate.sgv2.jsonapi.service.schema.versioning;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionRerankDef;

public class RerankDefSchemaValueDef extends SchemaValueDef<CollectionRerankDef> {

  @VisibleForTesting
  public static final RerankDefSchemaValueDef FOR_TESTING = new RerankDefSchemaValueDef(false);

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
}
