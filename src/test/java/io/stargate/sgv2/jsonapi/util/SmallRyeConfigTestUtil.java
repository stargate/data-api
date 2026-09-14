package io.stargate.sgv2.jsonapi.util;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import io.smallrye.config.WithParentName;
import io.smallrye.config.common.utils.StringUtil;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Utils for working with SmallRye config classes in tests, mostly so we can use method references
 * that the compiler can detect if they are invalid rather than string names for metrics.
 *
 * <p>See {@link #propertyName(ConfigMethodRef)} and {@link #addPropertyTo(Map, ConfigMethodRef,
 * String)}
 */
public final class SmallRyeConfigTestUtil {

  /**
   * Functional interface to represent a method on a SmallRye config interface.
   *
   * <p>{@link Serializable} is required, the java compiler only emits the synthetic <code>
   * writeReplace()</code> function for a lambda whose target type is serializable.
   *
   * <p>Used with {@link #propertyName(ConfigMethodRef)} , the T type param is the instance that
   * would be passed if the Function was evaluated. This is why it's a Function not a Supplier even
   * though the Config function takes no params.
   *
   * <pre>
   *      SmallRyeConfigUtil.propertyName(MyConfigInterface::host);
   *  </pre>
   *
   * @param <T> Type of the object that would be <code>this</code> if the function was invoked.
   * @param <R> Type of the return from the function
   */
  @FunctionalInterface
  public interface ConfigMethodRef<T, R> extends Function<T, R>, Serializable {}

  /**
   * Gets the string name for this config mapping method, so it can be used in tests without using
   * string consts.
   *
   * <p>You will probably want to use {@link #addPropertyTo(Map, ConfigMethodRef, String)}
   *
   * <p>Example, given this config:
   *
   * <pre>{@code
   * @ConfigMapping(prefix = "stargate.jsonapi.billing")
   * public interface BillingConfig {
   *     @WithDefault("serverless")
   *     String product();
   *  }
   *
   * }</pre>
   *
   * Invoked using:
   *
   * <pre>{@code
   * SmallRyeConfigUtil.propertyName(BillingConfig::product);
   *
   * }</pre>
   *
   * Will return the string:
   *
   * <pre>{@code
   * stargate.jsonapi.billing.product
   *
   * }</pre>
   */
  public static <T, R> String propertyName(ConfigMethodRef<T, R> methodRef) {

    var configMethod = configMethod(methodRef);
    // e.g. get the ConfigMapping instance from:
    // @ConfigMapping(prefix = "stargate.jsonapi.billing")
    // public interface BillingConfig {
    var configMappingAnnotation =
        configMethod.getDeclaringClass().getAnnotation(ConfigMapping.class);

    // pulls the "stargate.jsonapi.billing" from above example
    var propertyNameBuilder = new StringBuilder(configMappingAnnotation.prefix());

    if (!configMethod.isAnnotationPresent(WithParentName.class)) {
      if (!propertyNameBuilder.isEmpty()) {
        propertyNameBuilder.append('.');
      }
      propertyNameBuilder.append(
          propertyNameSegment(configMethod, configMappingAnnotation.namingStrategy()));
    }
    return propertyNameBuilder.toString();
  }

  /**
   * Helper method to add the property name and value to the map, when you need to build a map of
   * config for overriding in a test.
   *
   * <p>Example usage:
   *
   * <pre>{@code
   * Map<String, String> props = new HashMap<>();
   * addPropertyTo(props, BillingS3UploadConfig::enabled, true);
   * addPropertyTo(props, BillingS3UploadConfig::region, REGION);
   * addPropertyTo(props, BillingS3UploadConfig::bucket, BUCKET);
   *
   * }</pre>
   */
  public static <T, R> void addPropertyTo(
      Map<String, String> props, ConfigMethodRef<T, R> methodRef, String value) {
    props.put(propertyName(methodRef), value);
  }

  public static <T, R> void addPropertyTo(
      Map<String, String> props, ConfigMethodRef<T, R> methodRef, int value) {
    addPropertyTo(props, methodRef, Integer.toString(value));
  }

  public static <T, R> void addPropertyTo(
      Map<String, String> props, ConfigMethodRef<T, R> methodRef, boolean value) {
    addPropertyTo(props, methodRef, Boolean.toString(value));
  }

  private static String propertyNameSegment(
      Method configMethod, ConfigMapping.NamingStrategy namingStrategy) {

    // check if it is using a different name in config than the name of the method
    WithName withName = configMethod.getAnnotation(WithName.class);
    if (withName != null) {
      return withName.value();
    }

    return switch (namingStrategy) {
      case VERBATIM -> configMethod.getName();
        // skewer() is the same function SmallRye uses internally to derive the segment.
      case SNAKE_CASE -> StringUtil.skewer(configMethod.getName()).replace('-', '_');
      case KEBAB_CASE -> StringUtil.skewer(configMethod.getName());
    };
  }

  /** Gets the standard {@link Method} that the Function object is pointing to. */
  private static Method configMethod(ConfigMethodRef<?, ?> methodRef) {

    Objects.requireNonNull(methodRef, "methodRef must not be null");

    try {
      // writeReplace() is generated by javac on the lambda class, private and undeclared
      // in any source code; it returns the SerializedLambda holding the referenced
      // method's owner and name.
      Method writeReplace = methodRef.getClass().getDeclaredMethod("writeReplace");
      writeReplace.setAccessible(true);
      SerializedLambda serializedLambda = (SerializedLambda) writeReplace.invoke(methodRef);
      // getImplClass() returns the class-file form with slashes, e.g. "com/example/Server",
      // so convert it to the binary name "com.example.Server" that Class.forName expects.

      Class<?> configInterface = Class.forName(serializedLambda.getImplClass().replace('/', '.'));
      return configInterface.getMethod(serializedLambda.getImplMethodName());
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(e);
    }
  }
}
