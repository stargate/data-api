package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;

/** Factory for creating {@link CollectionOpenSearchDef} schema values. */
public class CollectionOpenSearchDefSchemaFactory
    extends CollectionSchemaFactory<CollectionOpenSearchDef> {

  @VisibleForTesting
  public static final CollectionOpenSearchDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionOpenSearchDefSchemaFactory();

  public CollectionOpenSearchDefSchemaFactory() {
    super(
        CollectionOpenSearchDef.class,
        CollectionOpenSearchDef.SCHEMA_DEFAULTS,
        CollectionSchemaVersion.V_3,
        CollectionSchemaVersion.V_3,
        false);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      io.stargate.sgv2.jsonapi.service.schema.SchemaVersion candidateVersion,
      CollectionOpenSearchDef candidatePersisted) {
    throw new UnsupportedOperationException("OpenSearch is not feature-gated");
  }
}
