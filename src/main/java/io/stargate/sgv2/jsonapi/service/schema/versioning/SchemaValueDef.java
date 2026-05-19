package io.stargate.sgv2.jsonapi.service.schema.versioning;

public abstract class SchemaValueDef<T> {

  private final Class<T> clazz;

  private final SchemaVersion releasedVersion;
  private final T preReleaseValue;

  private final SchemaVersion currentVersion;
  private final T currentDefault;

  private final boolean featureDisabled;
  private final T featureDisabledDefault;

  protected SchemaValueDef(
      Class<T> clazz,
      SchemaVersion releasedVersion,
      T preReleaseValue,
      SchemaVersion currentVersion,
      T currentDefault,
      boolean featureDisabled,
      T featureDisabledDefault) {
    this.clazz = clazz;
    this.releasedVersion = releasedVersion;
    this.preReleaseValue = preReleaseValue;
    this.currentVersion = currentVersion;
    this.currentDefault = currentDefault;
    this.featureDisabled = featureDisabled;
    this.featureDisabledDefault = featureDisabledDefault;
  }

  public SchemaValue<T> currentVersion(T persistedValue) {
    return create(SchemaVersion.CURRENT_VERSION, persistedValue);
  }

  public SchemaValue<T> namedVersion(SchemaVersion persistedVersion, T persistedValue) {

    if (persistedVersion.ordinalValue() < releasedVersion.ordinalValue()
        && persistedValue != null) {
      throw new IllegalArgumentException(
          "Persisted value must be null for pre-release version. persistedVersion=%s, persistedValue=%s, %s"
              .formatted(persistedVersion, persistedValue, errorContext()));
    }

    return create(persistedVersion, persistedValue);
  }

  protected SchemaValue<T> create(SchemaVersion persistedVersion, T persistedValue) {
    checkValidPersistedValue(persistedVersion, persistedValue);
    return new SchemaValue<>(this, persistedVersion, persistedValue);
  }

  protected void checkValidPersistedValue(SchemaVersion candidateVersion, T candidatePersisted) {

    // if the feature is disabled in this schema factory, then the persisted value MUST be value
    // equal
    // to the value we use when the feature is disabled.
    if (featureDisabled
        && (candidatePersisted != null && !candidatePersisted.equals(featureDisabledDefault))) {
      onInvalidValueFeatureDisabled(candidateVersion, candidatePersisted);
    }
  }

  protected abstract void onInvalidValueFeatureDisabled(
      SchemaVersion candidateVersion, T candidatePersisted);

  public T preReleaseValue() {
    return preReleaseValue;
  }

  public T currentDefault() {
    return currentDefault;
  }

  public SchemaVersion releasedVersion() {
    return releasedVersion;
  }

  public SchemaVersion currentVersion() {
    return currentVersion;
  }

  public Class<T> clazz() {
    return clazz;
  }

  protected T defaultForPersistedVersion(SchemaVersion persistedVersion) {
    if (persistedVersion.ordinalValue() < releasedVersion.ordinalValue()) {
      return preReleaseValue;
    }
    if (featureDisabled) {
      return featureDisabledDefault;
    }
    return currentDefault;
  }

  private String errorContext() {
    return "schema class=%s, currentVersion=%s, releasedVersion=%s, featureDisabled=%s"
        .formatted(clazz.getSimpleName(), currentVersion, releasedVersion, featureDisabled);
  }
}
