package io.stargate.sgv2.jsonapi.service.schema.versioning;

import java.util.Objects;

/**
 * A typed factory for creting {@link SchemaValue} instances, see the subclasses for details on
 * their configuration.
 *
 * <p>Using the factory to wrap an instance of an implementing <code>T</code> that is from either a
 * user or from disk, and may be null - this is called the "Persisted Value". Specific rules then
 * kick in to get a "Running Value" which is the value you are actually going to use in operations.
 *
 * <p><b>NOTE:</b> The factory needs to know if the feature is enabled, and this is normally done
 * via {@link io.stargate.sgv2.jsonapi.config.feature.ApiFeatures} which can be overridden per
 * reqest. So these factories need to be per request (or smart caching), see {@link
 * VersionedSchema}.
 *
 * <p>Here are the rules to follow:
 *
 * <ol>
 *   <li>To know what config value to use, always call {@link SchemaValue#runningValue()}
 *   <li>When reading a schema value from disk, use {@link #namedVersion(CollectionSchemaVersion,
 *       Object)} with the version of the on disk schema.
 *   <li>When creating a new schema value from the user, use {@link #currentVersion(Object)} with
 *       the value from the user
 * </ol>
 *
 * <p>Examples:
 *
 * <ul>
 *   <li>A collection may not have defined schema config for lexical on disk because it was created
 *       before, when you read this schema def from disk use {@link
 *       #namedVersion(CollectionSchemaVersion, Object)} because you know the name of the version
 *       and pass null. When making a decision about the lexcial config for that collection at query
 *       time use the {@link SchemaValue#runningValue()} method - this will see null persisted value
 *       and fall back to {@link SchemaFactory#defaultForPersistedVersion(CollectionSchemaVersion)}.
 *   <li>A user creates a new Collection, they did not pass options for lexical, so use {@link
 *       #currentVersion(Object)} and pass null. Then when {@link SchemaValue#runningValue()} is
 *       called it will see null persisted value and fall back to {@link
 *       SchemaFactory#defaultForPersistedVersion(CollectionSchemaVersion)} to get the current
 *       default.
 *   <li>In either of the above cases, if you have a non-null value make the same calls and the
 *       {@link SchemaValue#runningValue()} will see the non null persisted value and return it.
 * </ul>
 *
 * @param <T> The type of the Schema value that we need to create in the factory. Recommend this is
 *     a record, failing that it should be immutable and provide a sensible {@link
 *     Object#equals(Object)} implementation.
 */
public abstract class SchemaFactory<T> {

  private final Class<T> clazz;

  private final CollectionSchemaVersion releasedVersion;
  private final T preReleaseValue;

  private final CollectionSchemaVersion currentVersion;
  private final T currentDefault;

  private final boolean featureDisabled;
  private final T featureDisabledDefault;

  /**
   * Configure a new instance of the factory.
   *
   * @param clazz The class of the schema value that this factory will create.
   * @param releasedVersion The first version of schema that this feature was released in.
   * @param preReleaseValue The value of the configuration to use when the schema we have read is
   *     from before the <code>releasedVersion</code>
   * @param currentVersion The current version of the schema, should come from {@link
   *     CollectionSchemaVersion#CURRENT_VERSION}
   * @param currentDefault The value of the configuration to use for the current default, this will
   *     be the default used when creating a new schema value where the user has not specified a
   *     value.
   * @param featureDisabled Flag if the feature is disabled for this factory / request. For example,
   *     if lexical search is not available.
   * @param featureDisabledDefault The value of the configuration to use when the feature is
   *     disabled. As well as defaults this is used to check if a persisted value should be allowed
   *     then the feature is disabled, see {@link #checkValidPersistedValue(CollectionSchemaVersion,
   *     Object)}
   */
  protected SchemaFactory(
      Class<T> clazz,
      CollectionSchemaVersion releasedVersion,
      T preReleaseValue,
      CollectionSchemaVersion currentVersion,
      T currentDefault,
      boolean featureDisabled,
      T featureDisabledDefault) {
    this.clazz = Objects.requireNonNull(clazz, "clazz must not be null");
    this.releasedVersion =
        Objects.requireNonNull(releasedVersion, "releasedVersion must not be null");
    this.preReleaseValue =
        Objects.requireNonNull(preReleaseValue, "preReleaseValue must not be null");
    this.currentVersion = Objects.requireNonNull(currentVersion, "currentVersion must not be null");
    this.currentDefault = Objects.requireNonNull(currentDefault, "currentDefault must not be null");
    this.featureDisabled =
        Objects.requireNonNull(featureDisabled, "featureDisabled must not be null");
    this.featureDisabledDefault =
        Objects.requireNonNull(featureDisabledDefault, "featureDisabledDefault must not be null");
  }

  /**
   * Create a new {@link SchemaValue} for the current version of the schema, for use with user
   * supplied values.
   *
   * <p>Note: if the feature for this schema is disabled, a non-null value must be equal to the
   * {@link #featureDisabledDefault} value.Otherwise, a schema value dependany error is throw, see
   * subclasses
   *
   * @param persistedValue Nullable value that was supplied by the user.
   * @return A new {@link SchemaValue} for the current version of the schema.
   */
  public SchemaValue<T> currentVersion(T persistedValue) {
    return create(CollectionSchemaVersion.CURRENT_VERSION, persistedValue);
  }

  /**
   * Create a new {@link SchemaValue} for a specific version of the schema, for use when reading
   * schema from disk.
   *
   * <p>Note: if the feature for this schema is disabled, a non-null value must be equal to the
   * {@link #featureDisabledDefault} value.Otherwise, a schema value dependany error is throw, see
   * subclasses
   *
   * @param persistedVersion The version of the schema that was read from disk.
   * @param persistedValue Nullable value that was read from disk.
   * @return A new {@link SchemaValue} for the specific version of the schema.
   */
  public SchemaValue<T> namedVersion(CollectionSchemaVersion persistedVersion, T persistedValue) {

    if (persistedVersion.ordinalValue() < releasedVersion.ordinalValue()
        && persistedValue != null) {
      throw new IllegalArgumentException(
          "Persisted value must be null for pre-release version. persistedVersion=%s, persistedValue=%s, %s"
              .formatted(persistedVersion, persistedValue, errorContext()));
    }

    return create(persistedVersion, persistedValue);
  }

  /** Internal central factory for creation */
  protected SchemaValue<T> create(CollectionSchemaVersion persistedVersion, T persistedValue) {
    checkValidPersistedValue(persistedVersion, persistedValue);
    return new SchemaValue<>(this, persistedVersion, persistedValue);
  }

  protected void checkValidPersistedValue(
      CollectionSchemaVersion candidateVersion, T candidatePersisted) {

    // if the feature is disabled in this schema factory, then the persisted value MUST be value
    // equal to the value we use when the feature is disabled.
    if (featureDisabled
        && (candidatePersisted != null && !candidatePersisted.equals(featureDisabledDefault))) {
      onInvalidValueFeatureDisabled(candidateVersion, candidatePersisted);
    }
  }

  /**
   * Subclasses must implement this method, which will be called if the feature is disabled and a
   * non-null persisted value is provided that does not equal the {@link #featureDisabledDefault}
   * value.
   *
   * <p>Implementations should throw a relevant exception, see subclasses.
   */
  protected abstract void onInvalidValueFeatureDisabled(
      CollectionSchemaVersion candidateVersion, T candidatePersisted);

  /**
   * Get the default value to use, given a persisted version and the feature disabled state. This is
   * designed for use by {@link SchemaValue#runningValue()}
   *
   * @param persistedVersion Version of the schema in the {@link SchemaValue} enum.
   * @return The default value to use.
   */
  protected T defaultForPersistedVersion(CollectionSchemaVersion persistedVersion) {

    // Feature is disabled, the version does not matter
    if (featureDisabled) {
      return featureDisabledDefault;
    }

    // The version is from before the release, use prerelease
    if (persistedVersion.ordinalValue() < releasedVersion.ordinalValue()) {
      return preReleaseValue;
    }

    return currentDefault;
  }

  private String errorContext() {
    return "schema class=%s, currentVersion=%s, releasedVersion=%s, featureDisabled=%s"
        .formatted(clazz.getSimpleName(), currentVersion, releasedVersion, featureDisabled);
  }
}
