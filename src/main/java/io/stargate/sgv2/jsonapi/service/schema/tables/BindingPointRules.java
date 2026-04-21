package io.stargate.sgv2.jsonapi.service.schema.tables;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Collection of rules to be applied when binding types into different parts of the schema.
 *
 * <p>The different places where types can be bound are defined in the {@link TypeBindingPoint}
 * enum. Implement the {@link BindingPointRules} to create instances of your own rules for each of
 * the enum values. Use {@link DefaultTypeBindingRules} if all you need is a simple set of rules for
 * "isSupported"
 */
public class BindingPointRules<T extends BindingPointRules.BindingPointRule> {

  private final Map<TypeBindingPoint, T> rules;

  @SafeVarargs
  public BindingPointRules(T... rules) {

    Map<TypeBindingPoint, T> working = new HashMap<>();

    for (T rule : rules) {
      Objects.requireNonNull(rule, "rule must not be null");

      if (working.containsKey(rule.bindingPoint())) {
        throw new IllegalArgumentException(
            "rule for binding point " + rule.bindingPoint() + " already registered.");
      }
      working.put(rule.bindingPoint(), rule);
    }

    for (TypeBindingPoint bindingPoint : TypeBindingPoint.values()) {
      if (!working.containsKey(bindingPoint)) {
        throw new IllegalArgumentException(
            "BindingPointRules() - No rule defined for binding point: " + bindingPoint);
      }
    }
    this.rules = Map.copyOf(working);
  }

  /**
   * Get the rules for a specific binding point.
   *
   * @param bindingPoint the binding point for which to get the rules
   * @return Implementations should return a {@link BindingPointRule} subclass with the rules for
   *     this binding point.
   */
  public T rule(TypeBindingPoint bindingPoint) {
    Objects.requireNonNull(bindingPoint, "bindingPoint must not be null");
    // null check is done in the constructor, so this should never happen
    return rules.get(bindingPoint);
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
