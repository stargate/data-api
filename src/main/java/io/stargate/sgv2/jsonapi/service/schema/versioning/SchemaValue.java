package io.stargate.sgv2.jsonapi.service.schema.versioning;

import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaValue<T> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaValue.class);

  private final SchemaValueDef<T> defn;

  private final SchemaVersion persistedVersion;

  // can be null
  private final T persistedValue;

  SchemaValue(SchemaValueDef<T> defn, SchemaVersion persistedVersion, T persistedValue) {
    this.persistedVersion = persistedVersion;
    this.persistedValue = persistedValue;
    this.defn = defn;
  }

  /**
   * This is the value to use for operations that need this value, it IS NOT the persisted schema.
   * Use this for any DML sort of ops that just want to know what value to make decisions with
   *
   * @return
   */
  public T runningValue() {
    return persistedValue != null
        ? persistedValue
        : defn.defaultForPersistedVersion(persistedVersion);
  }

  public ReplaceDecision<T> replaceIfMissing(SchemaValue<T> replacement) {
    Objects.requireNonNull(replacement, "replacement must be null");

    if (persistedValue != null) {
      // we have a value, so no replacement.
      LOGGER.info(
          "replaceIfMissing() - this has persisted value, not replacing. this.persistedVersion()={}, this.persistedValue()={}, replacement.persistedVersion()={}, replacement.persistedValue()={}",
          persistedVersion,
          persistedValue,
          replacement.persistedVersion,
          replacement.persistedValue);
      return new ReplaceDecision<>(false, this);
    }

    // We take the replacement because a SchemaValue will **always** have a runningValue. So by
    // taking the
    // replacement we take its persisted value, OR the running value, which may be a default, such
    // as the
    // pre-release default.
    LOGGER.info(
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
   * will be used is equal"
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
