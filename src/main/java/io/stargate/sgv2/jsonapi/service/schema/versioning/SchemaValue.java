package io.stargate.sgv2.jsonapi.service.schema.versioning;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instance of a schema value created by a {@link SchemaFactory} subclass, see that class for
 * instructions on how to create a new instance.
 *
 * <p>Call {@link #runningValue()} to get the value that should be used for operations that need
 * this value.
 *
 * <p>If you have a value of schema from a user, which may be null, and a value from the disk /
 * existing collection call {@link #replaceIfMissing(SchemaValue)} to decide which value to use.
 *
 * @param <T> The type of the schema value
 */
public class SchemaValue<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValue.class);

  private final SchemaFactory<T> factory;

  private final CollectionSchemaVersion persistedVersion;

  // Nullable
  private final T persistedValue;

  SchemaValue(
      SchemaFactory<T> factory, CollectionSchemaVersion persistedVersion, T persistedValue) {
    this.persistedVersion =
        Objects.requireNonNull(persistedVersion, "persistedVersion must not be null");
    this.persistedValue = persistedValue;
    this.factory = Objects.requireNonNull(factory, "factory must not be null");
  }

  /**
   * This is the value to use for operations that need this value
   *
   * @return the Value of the schema value to use for regular operations.
   */
  public T runningValue() {
    return persistedValue != null
        ? persistedValue
        : factory.defaultForPersistedVersion(persistedVersion);
  }

  /**
   * Decides if this instance has a persisted value that should be used, or if the replacement
   * should be used.
   *
   * <p>This is useful when comparing schema from a user and what is already on disk. i.e. if the
   * user gave as a null value for schema, then this instance will have a null persisted value, and
   * when replacement is the value we got from disk we will use that. This allows for accurate
   * comparision of a createCollection command schema to an existing collection schema.
   *
   * @param replacement The value to use if this instance does not have a persisted value.
   * @return A decision on whether to use the replacement or this instance.
   */
  public ReplaceDecision<T> replaceIfMissing(SchemaValue<T> replacement) {
    Objects.requireNonNull(replacement, "replacement must be null");

    if (persistedValue != null) {
      // we have a value, so no replacement.
      LOGGER.trace(
          "replaceIfMissing() - this has persisted value, not replacing. this.persistedVersion()={}, this.persistedValue()={}, replacement.persistedVersion()={}, replacement.persistedValue()={}",
          persistedVersion,
          persistedValue,
          replacement.persistedVersion,
          replacement.persistedValue);
      return new ReplaceDecision<>(false, this);
    }

    // We take the replacement because a SchemaValue will **always** have a runningValue. So by
    // taking the replacement we take its persisted value, OR the running value, which may be a
    // default, such
    // as the pre-release default.
    LOGGER.trace(
        "replaceIfMissing() - this has null persisted value, replacing. this.persistedVersion()={}, replacement.persistedVersion()={}, replacement.persistedValue()={}, replacement.runningValue()={}",
        persistedVersion,
        replacement.persistedVersion,
        replacement.persistedValue,
        replacement.runningValue());
    return new ReplaceDecision<>(true, replacement);
  }

  /**
   * Two values are ONLY equal if their running values are equal, that means a persisted value may
   * be compared to a current default. Which is fine, we want to say "the actual schema value that
   * will be used is equal."
   *
   * @param obj the reference object with which to compare.
   * @return
   */
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SchemaValue<?> other) {
      return Objects.equals(runningValue(), other.runningValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(runningValue());
  }

  public record ReplaceDecision<T>(boolean isReplacement, SchemaValue<T> value) {}
}
