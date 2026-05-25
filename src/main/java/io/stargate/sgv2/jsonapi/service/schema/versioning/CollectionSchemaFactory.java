package io.stargate.sgv2.jsonapi.service.schema.versioning;

/**
 * Base for all SchmeaFactories for collection schema, so we set the current version in one place.
 */
public abstract class CollectionSchemaFactory<T> extends SchemaFactory<T> {

  protected CollectionSchemaFactory(
      Class<T> clazz,
      SchemaDefaults<T> schemaDefaults,
      SchemaVersion releasedVersion,
      SchemaVersion currentVersion,
      boolean featureDisabled) {
    super(clazz, schemaDefaults, releasedVersion, currentVersion, featureDisabled);
  }

  @Override
  protected SchemaVersion currentVersion() {
    return CollectionSchemaVersion.CURRENT_VERSION;
  }
}
