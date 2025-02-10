package io.stargate.sgv2.jsonapi.api.model.command;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import io.stargate.sgv2.jsonapi.config.constants.ApiConstants;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Source of and cache for all Config objects for a {@link CommandContext}.
 *
 * <p>We do this to avoid having to pass the config object around to all the classes that need it,
 * and avoid using injection all through the commands. You can get a config object using {@link
 * #get(Class)}
 *
 * <p>Normally get the config via {@link CommandContext#config()}
 *
 * <p>Create a single instance and call {@link #preLoadConfigs(List)} with all the config classes
 * you want to preload at start up to ensure they can be read and so they are logged at startup. Any
 * interfaces not preloaded will be loaded on demand when used and cached..
 *
 * <p>See {@link io.stargate.sgv2.jsonapi.ConfigPreLoader}
 */
public class CommandConfig {

  private static final Logger LOGGER = LoggerFactory.getLogger(CommandConfig.class);

  private final ConcurrentMap<Class<?>, Object> configCache = new ConcurrentHashMap<>();

  // use getConfigProvider()
  private static SmallRyeConfig _config_provider = null;

  /**
   * Call to preload and log the config classes.
   *
   * @param configClasses List of config classes to preload
   */
  public void preLoadConfigs(List<Class<?>> configClasses) {
    for (Class<?> configClass : configClasses) {
      LOGGER.info("Pre-loaded config class configClass={}", configClass.toString());
      var config = get(configClass);
      LOGGER.info("Pre-loaded config - {}", configMappingToString(configClass, config));
    }
  }

  /**
   * Uses the config service to populate the config interface passed in.
   *
   * <p>Example: <code>
   *   bool isDebugMode = get(DebugModeConfig.class).enabled()
   * </code>
   *
   * @param configType The configuration interface to populate, decorated with {@link
   *     io.smallrye.config.ConfigMapping}
   * @return Populated configration object of type <code>configType</code>
   * @param <ConfigType> The configuration interface to populate
   */
  @SuppressWarnings("unchecked")
  public <ConfigType> ConfigType get(Class<ConfigType> configType) {
    return (ConfigType)
        configCache.computeIfAbsent(
            configType, k -> getConfigProvider().getConfigMapping(configType));
  }

  /**
   * Gets the config service to use, depends on the offline mode.
   *
   * <p>TODO: Copied from JsonAPIException , not sure why we need to do this
   */
  private static SmallRyeConfig getConfigProvider() {
    // aaron - copied from JsonAPIException , not sure why we need to do this
    // TODO - cleanup how we get config, this seem unnecessary complicated

    if (_config_provider == null) {
      _config_provider =
          ApiConstants.isOffline()
              ? new SmallRyeConfigBuilder().build()
              : ConfigProvider.getConfig().unwrap(SmallRyeConfig.class);
    }
    return _config_provider;
  }

  /**
   * Private helper to iterate all the property getter functions on the config object and build a
   * nice string.
   */
  private static String configMappingToString(Class<?> configClass, Object configObject) {
    String properties =
        Arrays.stream(configClass.getDeclaredMethods())
            // Ignore default methods like toString(), equals(), hashCode()
            .filter(
                method ->
                    method.getParameterCount() == 0
                        && !"toString".equals(method.getName())
                        && !"equals".equals(method.getName())
                        && !"hashCode".equals(method.getName()))
            .map(
                method -> {
                  try {
                    Object value = method.invoke(configObject);
                    return method.getName() + "=" + valueToString(value);
                  } catch (Exception e) {
                    return method.getName() + "=<error>";
                  }
                })
            .collect(Collectors.joining(", "));
    return configClass.getSimpleName() + "{" + properties + "}";
  }

  /**
   * If the value is from our stargate package, then it is some sort of sub object to iterate it.
   */
  private static String valueToString(Object value) {
    if (value.getClass().getPackageName().startsWith("io.stargate")) {
      return configMappingToString(value.getClass(), value);
    } else {
      return value.toString();
    }
  }
}
