package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.config.feature.ApiFeatures;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.SchemaRegistry;
import io.stargate.sgv2.jsonapi.service.schema.SchemaVersion;

/**
 * Factory for creating the {@link CollectionLexicalDef} as a schema value, access via the {@link
 * SchemaRegistry}
 */
public class CollectionLexicalDefSchemaFactory
    extends CollectionSchemaFactory<CollectionLexicalDef> {

  /** Use this only for testing, it ignores the {@link ApiFeatures} config. */
  @VisibleForTesting
  public static final CollectionLexicalDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionLexicalDefSchemaFactory(false);

  /** Use this only for testing, it ignores the {@link ApiFeatures} config. */
  @VisibleForTesting
  public static final CollectionLexicalDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionLexicalDefSchemaFactory(true);

  public CollectionLexicalDefSchemaFactory(boolean featureDisabled) {
    super(
        CollectionLexicalDef.class,
        CollectionLexicalDef.SCHEMA_DEFAULTS,
        CollectionSchemaVersion.V_2,
        CollectionSchemaVersion.V_2,
        featureDisabled);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      SchemaVersion candidateVersion, CollectionLexicalDef candidatePersisted) {
    throw SchemaException.Code.LEXICAL_FEATURE_NOT_ENABLED.get();
  }
}
