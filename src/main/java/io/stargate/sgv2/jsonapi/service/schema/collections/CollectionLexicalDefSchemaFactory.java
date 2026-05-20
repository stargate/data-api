package io.stargate.sgv2.jsonapi.service.schema.collections;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.exception.SchemaException;
import io.stargate.sgv2.jsonapi.service.schema.versioning.CollectionSchemaVersion;
import io.stargate.sgv2.jsonapi.service.schema.versioning.SchemaFactory;
import io.stargate.sgv2.jsonapi.service.schema.versioning.VersionedSchema;

/**
 * Factory for creating the {@link CollectionLexicalDef} as a schema value, access via the {@link
 * VersionedSchema}
 */
public class CollectionLexicalDefSchemaFactory extends SchemaFactory<CollectionLexicalDef> {

  @VisibleForTesting
  public static final CollectionLexicalDefSchemaFactory FOR_TESTING_ENABLED =
      new CollectionLexicalDefSchemaFactory(false);

  @VisibleForTesting
  public static final CollectionLexicalDefSchemaFactory FOR_TESTING_DISABLED =
      new CollectionLexicalDefSchemaFactory(true);

  public CollectionLexicalDefSchemaFactory(boolean featureDisabled) {
    super(
        CollectionLexicalDef.class,
        CollectionSchemaVersion.V_2,
        CollectionLexicalDef.configForPreLexical(),
        CollectionSchemaVersion.V_2,
        CollectionLexicalDef.configForDefault(),
        featureDisabled,
        CollectionLexicalDef.LEXICAL_DISABLED);
  }

  @Override
  protected void onInvalidValueFeatureDisabled(
      CollectionSchemaVersion candidateVersion, CollectionLexicalDef candidatePersisted) {
    throw SchemaException.Code.LEXICAL_FEATURE_NOT_ENABLED.get();
  }
}
