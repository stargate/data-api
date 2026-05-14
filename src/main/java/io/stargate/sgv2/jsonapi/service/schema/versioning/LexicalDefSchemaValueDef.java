package io.stargate.sgv2.jsonapi.service.schema.versioning;

import com.google.common.annotations.VisibleForTesting;
import io.stargate.sgv2.jsonapi.service.schema.collections.CollectionLexicalDef;

public class LexicalDefSchemaValueDef extends SchemaValueDef<CollectionLexicalDef> {

  @VisibleForTesting
  public static final LexicalDefSchemaValueDef FOR_TESTING_ENABLED =
      new LexicalDefSchemaValueDef(false);

  @VisibleForTesting
  public static final LexicalDefSchemaValueDef FOR_TESTING_DISABLED =
      new LexicalDefSchemaValueDef(true);

  LexicalDefSchemaValueDef(boolean featureDisabled) {
    super(
        CollectionLexicalDef.class,
        SchemaVersion.V_2,
        CollectionLexicalDef.configForPreLexical(),
        SchemaVersion.V_2,
        CollectionLexicalDef.configForDefault(),
        featureDisabled,
        CollectionLexicalDef.LEXICAL_DISABLED);
  }
}
