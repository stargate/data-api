package io.stargate.sgv2.jsonapi.service.schema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SchemaHolder} and {@link SchemaFactory}.
 *
 * <p>NOTE: the {@link CreateCollectionVsDiskScenario} describes end to end flows that will happen
 * when a user is creating a colleciton.
 *
 * <p>These tests use a minimal in-package fixture — {@link FixtureFactory} — that makes it easy to
 * see exactly what the factory is configured with. Every test method explains the scenario in plain
 * English before asserting, so the tests double as documentation.
 *
 * <h2>Vocabulary</h2>
 *
 * <ul>
 *   <li><b>persisted value</b> – the raw value stored on disk (or supplied by the user). May be
 *       {@code null} when the field was absent.
 *   <li><b>running value</b> – the effective value used at query time; the persisted value when
 *       present, otherwise a default chosen by the factory.
 *   <li><b>pre-release default</b> – the value used for schema written before the feature existed.
 *   <li><b>current default</b> – the default applied when a feature is released but the user did
 *       not specify a value.
 *   <li><b>disabled-feature value</b> – the value used when the feature is explicitly turned off in
 *       this environment.
 * </ul>
 */
class SchemaHolderAndFactoryTest {

  // ─── Shared test values ────────────────────────────────────────────────────

  /** A plain string wrapper so we can test generics without a real schema class. */
  record Val(String name) {}

  static final Val PRE_RELEASE_DEFAULT = new Val("pre-release-default");
  static final Val CURRENT_DEFAULT = new Val("current-default");
  static final Val DISABLED_FEATURE = new Val("disabled-feature");
  static final Val USER_VALUE = new Val("user-value");
  static final Val DISK_VALUE = new Val("disk-value");

  // ─── Version enum used by the fixture ──────────────────────────────────────

  /**
   * Three-value version enum that mimics {@link CollectionSchemaVersion}:
   *
   * <ul>
   *   <li>{@code OLD} – existed before the feature was released (ordinal 0)
   *   <li>{@code RELEASED} – the version the feature first shipped in (ordinal 1)
   *   <li>{@code CURRENT} – the latest version (ordinal 2)
   * </ul>
   */
  enum TestVersion implements SchemaVersion {
    OLD(0),
    RELEASED(1),
    CURRENT(2);

    private final int ordinal;

    TestVersion(int ordinal) {
      this.ordinal = ordinal;
    }

    @Override
    public int ordinalValue() {
      return ordinal;
    }

    @Override
    public String toString() {
      return String.valueOf(ordinalValue());
    }
  }

  // ─── Fixture SchemaDefaults ─────────────────────────────────────────────────

  static final SchemaDefaults<Val> DEFAULTS =
      new SchemaDefaults<>() {
        @Override
        public Val forPreRelease() {
          return PRE_RELEASE_DEFAULT;
        }

        @Override
        public Val currentDefault() {
          return CURRENT_DEFAULT;
        }

        @Override
        public Val forDisabledFeature() {
          return DISABLED_FEATURE;
        }
      };

  // ─── Fixture SchemaFactory ──────────────────────────────────────────────────

  /**
   * Minimal concrete {@link SchemaFactory} for tests.
   *
   * <ul>
   *   <li>{@code releasedVersion} = {@link TestVersion#RELEASED}
   *   <li>{@code currentVersion} = {@link TestVersion#CURRENT}
   *   <li>{@code featureDisabled} is configurable per test
   * </ul>
   */
  static class FixtureFactory extends SchemaFactory<Val> {

    FixtureFactory(boolean featureDisabled) {
      super(
          Val.class,
          DEFAULTS,
          TestVersion.RELEASED, // feature first existed here
          TestVersion.CURRENT, // create new holders at this version
          featureDisabled);
    }

    @Override
    protected SchemaVersion currentVersion() {
      return TestVersion.CURRENT;
    }

    @Override
    protected void onInvalidValueFeatureDisabled(SchemaVersion version, Val value) {
      throw new IllegalStateException(
          "Feature is disabled but received non-disabled value: "
              + value
              + " at version "
              + version);
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // SchemaHolder.runningValue()
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class RunningValue {

    /**
     * When a holder has a non-null persisted value the running value is that exact persisted value
     * — no defaulting occurs.
     */
    @Test
    void returnsPersistedValueWhenPresent() {
      var factory = new FixtureFactory(false);
      var holder = factory.currentVersion(USER_VALUE);

      assertThat(holder.runningValue()).isSameAs(USER_VALUE);
    }

    /**
     * When the persisted value is null the factory's {@code defaultForPersistedVersion} is called.
     * For a CURRENT-version holder with the feature enabled the result is {@link #CURRENT_DEFAULT}.
     */
    @Test
    void fallsBackToCurrentDefaultWhenPersistedValueIsNull_featureEnabled() {
      var factory = new FixtureFactory(false);
      var holder = factory.currentVersion(null); // user did not supply a value

      assertThat(holder.runningValue()).isSameAs(CURRENT_DEFAULT);
    }

    /**
     * When the persisted value is null and the holder's version is older than the release version
     * the factory returns the pre-release default.
     */
    @Test
    void fallsBackToPreReleaseDefaultForOldSchema() {
      var factory = new FixtureFactory(false);
      // OLD is ordinal 0, RELEASED is ordinal 1 — so OLD < RELEASED
      var holder = factory.namedVersion(TestVersion.OLD, null);

      assertThat(holder.runningValue()).isSameAs(PRE_RELEASE_DEFAULT);
    }

    /**
     * When the feature is disabled the running value is always the disabled-feature value,
     * regardless of the persisted version.
     */
    @Test
    void returnsDisabledFeatureValueWhenFeatureIsDisabled_nullPersistedValue() {
      var factory = new FixtureFactory(true); // feature OFF
      var holder = factory.currentVersion(null);

      assertThat(holder.runningValue()).isSameAs(DISABLED_FEATURE);
    }

    /**
     * When the feature is disabled and the persisted value equals the disabled-feature sentinel the
     * running value is that sentinel (not a secondary default).
     */
    @Test
    void returnsDisabledFeatureValueWhenFeatureIsDisabled_persistedValueEqualsDisabled() {
      var factory = new FixtureFactory(true); // feature OFF
      // DISABLED_FEATURE is the only allowed non-null value when feature is off
      var holder = factory.currentVersion(DISABLED_FEATURE);

      assertThat(holder.runningValue()).isSameAs(DISABLED_FEATURE);
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // SchemaHolder.equals() and hashCode()
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class Equality {

    /**
     * Two holders are equal when their running values are equal — even if one has an explicit
     * persisted value and the other falls back to the same default.
     */
    @Test
    void equalWhenRunningValuesAreEqual() {
      var factory = new FixtureFactory(false);
      var holderWithExplicitValue = factory.currentVersion(CURRENT_DEFAULT);
      var holderWithNullValue = factory.currentVersion(null); // will default to CURRENT_DEFAULT

      assertThat(holderWithExplicitValue).isEqualTo(holderWithNullValue);
    }

    /** Two holders whose running values differ are not equal. */
    @Test
    void notEqualWhenRunningValuesDiffer() {
      var factory = new FixtureFactory(false);
      var holderA = factory.currentVersion(USER_VALUE);
      var holderB = factory.currentVersion(null); // defaults to CURRENT_DEFAULT

      assertThat(holderA).isNotEqualTo(holderB);
    }

    /** Hash codes are consistent with equality: equal running values → equal hash codes. */
    @Test
    void hashCodeConsistentWithEquals() {
      var factory = new FixtureFactory(false);
      var holderA = factory.currentVersion(CURRENT_DEFAULT);
      var holderB = factory.currentVersion(null);

      assertThat(holderA.hashCode()).isEqualTo(holderB.hashCode());
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // SchemaHolder.replaceIfMissing()
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class ReplaceIfMissing {

    /**
     * When "this" holder has a non-null persisted value it keeps itself — no replacement happens.
     * The returned decision records {@code isReplacement=false}.
     */
    @Test
    void keepsItselfWhenPersistedValueIsPresent() {
      var factory = new FixtureFactory(false);
      var fromUser = factory.currentVersion(USER_VALUE); // has a value
      var fromDisk = factory.currentVersion(DISK_VALUE); // would be the replacement

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isFalse();
      assertThat(decision.value()).isSameAs(fromUser);
    }

    /**
     * When "this" holder has a null persisted value it defers to the replacement. This is the
     * primary use case: the user did not specify a value, so we take the value from disk.
     */
    @Test
    void takesReplacementWhenPersistedValueIsNull() {
      var factory = new FixtureFactory(false);
      var fromUser = factory.currentVersion(null); // user omitted the field
      var fromDisk = factory.currentVersion(DISK_VALUE);

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isTrue();
      assertThat(decision.value()).isSameAs(fromDisk);
    }

    /**
     * Even when "this" has null and the replacement also has null, replacement is still chosen. The
     * running value will then be a default from the replacement's factory.
     */
    @Test
    void takesReplacementEvenWhenBothPersistedValuesAreNull() {
      var factory = new FixtureFactory(false);
      var fromUser = factory.currentVersion(null);
      var fromDisk = factory.currentVersion(null);

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isTrue();
      assertThat(decision.value()).isSameAs(fromDisk);
      // Both sides default to CURRENT_DEFAULT so running value is still deterministic
      assertThat(decision.value().runningValue()).isSameAs(CURRENT_DEFAULT);
    }

    /** Null replacement is not allowed — the method documents it must not be null. */
    @Test
    void throwsWhenReplacementIsNull() {
      var factory = new FixtureFactory(false);
      var holder = factory.currentVersion(null);

      assertThatThrownBy(() -> holder.replaceIfMissing(null))
          .isInstanceOf(NullPointerException.class);
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // SchemaFactory.currentVersion()
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class FactoryCurrentVersion {

    /** A non-null value supplied by the user is stored as-is and returned as the running value. */
    @Test
    void storesNonNullUserValue() {
      var factory = new FixtureFactory(false);
      var holder = factory.currentVersion(USER_VALUE);

      assertThat(holder.runningValue()).isSameAs(USER_VALUE);
    }

    /**
     * A null value (user omitted the field) is stored and causes the running value to fall back to
     * the current default.
     */
    @Test
    void nullValueFallsBackToCurrentDefault() {
      var factory = new FixtureFactory(false);
      var holder = factory.currentVersion(null);

      assertThat(holder.runningValue()).isSameAs(CURRENT_DEFAULT);
    }

    /**
     * When the feature is disabled and the user supplies a value that does NOT equal the
     * disabled-feature sentinel the factory rejects it.
     */
    @Test
    void throwsWhenFeatureDisabledAndUserProvidesIncompatibleValue() {
      var factory = new FixtureFactory(true); // feature OFF

      assertThatThrownBy(() -> factory.currentVersion(USER_VALUE))
          .isInstanceOf(IllegalStateException.class);
    }

    /**
     * When the feature is disabled supplying {@code null} is always safe — the running value
     * becomes the disabled-feature sentinel.
     */
    @Test
    void allowsNullWhenFeatureDisabled() {
      var factory = new FixtureFactory(true);
      var holder = factory.currentVersion(null);

      assertThat(holder.runningValue()).isSameAs(DISABLED_FEATURE);
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // SchemaFactory.namedVersion()
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class FactoryNamedVersion {

    /**
     * Reading a value from disk at the current version with a non-null persisted value: running
     * value is the persisted value.
     */
    @Test
    void diskValueAtCurrentVersionReturnedAsIs() {
      var factory = new FixtureFactory(false);
      var holder = factory.namedVersion(TestVersion.CURRENT, DISK_VALUE);

      assertThat(holder.runningValue()).isSameAs(DISK_VALUE);
    }

    /**
     * Reading a value from disk at the released version with a non-null persisted value: running
     * value is the persisted value (the feature existed at this version).
     */
    @Test
    void diskValueAtReleasedVersionReturnedAsIs() {
      var factory = new FixtureFactory(false);
      var holder = factory.namedVersion(TestVersion.RELEASED, DISK_VALUE);

      assertThat(holder.runningValue()).isSameAs(DISK_VALUE);
    }

    /**
     * Reading schema from disk written before the feature existed ({@code OLD} version), with null
     * (field was absent): running value is the pre-release default.
     */
    @Test
    void nullAtOldVersionFallsBackToPreReleaseDefault() {
      var factory = new FixtureFactory(false);
      var holder = factory.namedVersion(TestVersion.OLD, null);

      assertThat(holder.runningValue()).isSameAs(PRE_RELEASE_DEFAULT);
    }

    /**
     * A non-null value at a pre-release version is illegal — the feature did not exist yet, so
     * there should be nothing persisted.
     */
    @Test
    void throwsWhenNonNullValueAtPreReleaseVersion() {
      var factory = new FixtureFactory(false);

      assertThatThrownBy(() -> factory.namedVersion(TestVersion.OLD, DISK_VALUE))
          .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * When the feature is disabled and the disk has a value equal to the disabled-feature sentinel
     * the factory accepts it without error.
     */
    @Test
    void allowsDisabledSentinelWhenFeatureDisabled() {
      var factory = new FixtureFactory(true);
      var holder = factory.namedVersion(TestVersion.CURRENT, DISABLED_FEATURE);

      assertThat(holder.runningValue()).isSameAs(DISABLED_FEATURE);
    }

    /**
     * When the feature is disabled and the disk value is something other than the disabled-feature
     * sentinel the factory rejects it.
     */
    @Test
    void throwsWhenFeatureDisabledAndDiskValueIsIncompatible() {
      var factory = new FixtureFactory(true);

      assertThatThrownBy(() -> factory.namedVersion(TestVersion.CURRENT, DISK_VALUE))
          .isInstanceOf(IllegalStateException.class);
    }
  }

  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  // End-to-end scenario: user creates a collection, then we compare to disk
  // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  @Nested
  class CreateCollectionVsDiskScenario {

    /**
     * Scenario: user creates a collection WITHOUT specifying this field. The existing collection on
     * disk DOES have a value. We should end up using the disk value for comparison purposes.
     *
     * <p>This is exactly what {@code replaceIfMissing} is for.
     */
    @Test
    void userOmitsField_diskHasValue_useDiskValueForComparison() {
      var factory = new FixtureFactory(false);

      var fromUser = factory.currentVersion(null); // user did not specify
      var fromDisk = factory.namedVersion(TestVersion.CURRENT, DISK_VALUE);

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isTrue();
      assertThat(decision.value().runningValue()).isSameAs(DISK_VALUE);
    }

    /**
     * Scenario: user creates a collection AND specifies this field. The existing collection on disk
     * also has a value. The user's explicit value takes precedence.
     */
    @Test
    void userSpecifiesField_diskHasValue_useUserValue() {
      var factory = new FixtureFactory(false);

      var fromUser = factory.currentVersion(USER_VALUE);
      var fromDisk = factory.namedVersion(TestVersion.CURRENT, DISK_VALUE);

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isFalse();
      assertThat(decision.value().runningValue()).isSameAs(USER_VALUE);
    }

    /**
     * Scenario: user creates a collection without specifying the field. The collection on disk is
     * old (pre-feature). Both sides default — user to CURRENT_DEFAULT, disk to PRE_RELEASE_DEFAULT.
     * After replacement the running value is the pre-release default (from disk).
     */
    @Test
    void userOmitsField_diskIsOldSchema_usePreReleaseDefault() {
      var factory = new FixtureFactory(false);

      var fromUser = factory.currentVersion(null);
      var fromDisk = factory.namedVersion(TestVersion.OLD, null); // very old, no value

      var decision = fromUser.replaceIfMissing(fromDisk);

      assertThat(decision.isReplacement()).isTrue();
      assertThat(decision.value().runningValue()).isSameAs(PRE_RELEASE_DEFAULT);
    }
  }
}
