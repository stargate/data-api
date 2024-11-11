package io.stargate.sgv2.jsonapi.exception;

import com.google.common.base.Preconditions;
import io.stargate.sgv2.jsonapi.service.operation.OperationAttempt;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Re-usable class for holding an object and the {@link WarningException}'s that have been generated
 * for it.
 *
 * <p>This is usually used when analysing part of a query and generating warnings for the user.
 *
 * <p>Is a {@link Consumer} of {@link OperationAttempt} so that it can add the warnings to the
 * attempt, and so multiple instances can be chained together using {@link
 * Consumer#andThen(Consumer)}
 *
 * @param <T> Type of the target object the warnings are about.
 */
public class WithWarnings<T> implements Consumer<OperationAttempt<?, ?>> {

  private final T target;
  private final List<WarningException> warnings;
  private final List<WarningException.Code> suppressedWarnings;

  public WithWarnings(
      T target, List<WarningException> warnings, List<WarningException.Code> suppressedWarnings) {
    Preconditions.checkNotNull(target, "target must not be null");
    this.target = target;
    this.warnings = warnings == null ? new ArrayList<>() : warnings;
    this.suppressedWarnings = suppressedWarnings == null ? new ArrayList<>() : suppressedWarnings;
  }

  /**
   * The target object the warnings are about.
   *
   * @return The target object.
   */
  public T target() {
    return target;
  }

  /**
   * The warnings generated for the target object.
   *
   * <p>This is a mutable, so you can add more warnings to it.
   *
   * @return The list of warnings, never null.
   */
  public List<WarningException> warnings() {
    return warnings;
  }

  /**
   * The suppressed warnings generated for the target object.
   *
   * <p>This is a mutable, so you can add more suppressed warnings to it.
   *
   * @return The list of warnings, never null.
   */
  public List<WarningException.Code> suppressedWarnings() {
    return suppressedWarnings;
  }

  /** Returns true if there are no warnings. */
  public boolean isEmpty() {
    return warnings.isEmpty();
  }

  /*
   * Constructor an instance with no warnings.
   * @param target the target object that has no warnings
   * @return an instance with no warnings
   */
  public static <T> WithWarnings<T> of(T target) {
    return new WithWarnings<>(target, new ArrayList<>(), null);
  }

  /*
   * Constructor an instance with no warnings.
   * @param target the target object that has no warnings
   *
   * @return an instance with no warnings
   */
  public static <T> WithWarnings<T> of(T target, List<WarningException.Code> suppressedWarnings) {
    return new WithWarnings<>(target, new ArrayList<>(), suppressedWarnings);
  }

  /**
   * Constructor an instance with a single warning.
   *
   * @param target the target object that has the warning
   * @param warning the warning to add
   * @return An instance with the warning
   * @param <T> Type of the target object the warnings are about.
   */
  public static <T> WithWarnings<T> of(T target, WarningException warning) {
    Objects.requireNonNull(warning, "warning is required");
    var warnings = new ArrayList<WarningException>();
    warnings.add(warning);
    return new WithWarnings<>(target, warnings, null);
  }

  /**
   * Adds all the warnings to the {@link OperationAttempt}
   *
   * @param operationAttempt the {@link OperationAttempt} to add the warnings to
   */
  @Override
  public void accept(OperationAttempt<?, ?> operationAttempt) {
    Objects.requireNonNull(operationAttempt, "operationAttempt must not be null");
    warnings.forEach(operationAttempt::addWarning);
    suppressedWarnings.forEach(operationAttempt::addSuppressedWarning);
  }
}
