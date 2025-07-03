package io.stargate.sgv2.jsonapi.service.schema.tables;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Collection of rules to be applied when binding types into different parts of the schema.
 *
 * <p>The different places where types can be bound are defined in the {@link TypeBindingPoint}
 * enum. Implement the {@link BindingPointRules} to create instances of your own rules for each of
 * the enum values.
 */
public abstract class BindingPointRules<T extends BindingPointRules.BindingPointRule> {

  private final Map<TypeBindingPoint, T> rules = new HashMap<>();

  @SafeVarargs
  protected BindingPointRules(T... rules) {
    for (T rule : rules) {
      register(rule);
    }
  }

  private void register(T rule) {
    Objects.requireNonNull(rule, "rule must not be null");
    if (rules.containsKey(rule.bindingPoint())) {
      throw new IllegalArgumentException(
          "rule for binding point " + rule.bindingPoint() + " already registered.");
    }
    rules.put(rule.bindingPoint(), rule);
  }

  /**
   * Get the rules for a specific binding point.
   *
   * @param bindingPoint the binding point for which to get the rules
   * @return Implementations should return a {@link BindingPointRule} subclass with the rules for
   *     this binding point.
   * @throws IllegalArgumentException if the binding point is not supported by this implementation.
   */
  public T forBindingPoint(TypeBindingPoint bindingPoint) {
    Objects.requireNonNull(bindingPoint, "bindingPoint must not be null");
    T rule = rules.get(bindingPoint);
    if (rule == null) {
      throw new IllegalArgumentException("No rules defined for binding point: " + bindingPoint);
    }
    return rule;
  }

  /**
   * Common interface for binding point rules.
   *
   * <p>Implementations of this interface should have all the properties and methods they need for
   * the use case, this is just common interface to group them.
   */
  public interface BindingPointRule {

    /** The binding point for which these rules apply. */
    TypeBindingPoint bindingPoint();
  }
}
