package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.SchemaVersion;

/** Factory for creating {@link CollectionOpenSearchDef} schema values. */
public class CollectionOpenSearchDefSchemaFactory
    extends CollectionSchemaFactory<CollectionOpenSearchDef> {

  @VisibleForTesting
  public static final CollectionOpenSearchDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionOpenSearchDefSchemaFactory(false);

  @VisibleForTesting
  public static final CollectionOpenSearchDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionOpenSearchDefSchemaFactory(true);

  public CollectionOpenSearchDefSchemaFactory() {
    this(false);
  }

  public CollectionOpenSearchDefSchemaFactory(boolean featureDisabled) {
    super(
        CollectionOpenSearchDef.class,
        CollectionOpenSearchDef.SCHEMA_DEFAULTS,
        CollectionSchemaVersion.V_3,
        CollectionSchemaVersion.V_3,
        featureDisabled);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      SchemaVersion candidateVersion, CollectionOpenSearchDef candidatePersisted) {
    throw SchemaException.Code.OPEN_SEARCH_FEATURE_NOT_ENABLED.get();
  }
}
