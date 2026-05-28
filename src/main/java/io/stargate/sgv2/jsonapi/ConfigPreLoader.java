package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.ConfigMapping;
import io.stargate.sgv2.jsonapi.api.model.command.CommandConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jboss.jandex.ClassInfo;
import org.jboss.jandex.DotName;
import org.jboss.jandex.Index;
import org.jboss.jandex.IndexReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * On Quarkus application startup this will preload all the config interfaces in <code>
 * io.stargate.sgv2.jsonapi.config;</code> and sub packages that are decorated with <code>
 * @ConfigMapping</code> into a single instance of {@link
 * io.stargate.sgv2.jsonapi.api.model.command.CommandConfig} that can be used when building the
 * {@link io.stargate.sgv2.jsonapi.api.model.command.CommandContext} for the command execution.
 *
 * <p>Do this so we can log the config nicely and make it easier to confirm the config is valid.
 *
 * <p>Use {@link #getPreLoadOrEmpty()} to get the preloaded config or a new empty one if none
 * preloaded, when making a {@link io.stargate.sgv2.jsonapi.api.model.command.CommandContext}.
 */
@ApplicationScoped
public class ConfigPreLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPreLoader.class);

  private static final String JANDEX_LOCATION = "META-INF/jandex.idx";
  private static final String CONFIG_PACKAGE = "io.stargate.sgv2.jsonapi.config";

  private static final ReadWriteLock RW_LOCK = new ReentrantReadWriteLock();

  // Called from the quarkus StartupEvent handler, but there may be other functions
  // like RequestContext that call getPreLoadOrEmpty in a unit test so onStart is not run.
  // NOTE: not using volatile because releasing the writeLock() is a write barrier that forces
  // the memory to flush.
  private static CommandConfig commonConfig;

  public static CommandConfig getPreLoadOrEmpty() {
    RW_LOCK.readLock().lock();
    try {
      return commonConfig != null ? commonConfig : new CommandConfig();
    } finally {
      RW_LOCK.readLock().unlock();
    }
  }

  void onStart(@Observes StartupEvent event) {

    LOGGER.debug("onStart event - started pre loading all config interfaces");

    CommandConfig local = new CommandConfig();
    local.preLoadConfigs(getConfigInterfaces());

    RW_LOCK.writeLock().lock();
    try {
      commonConfig = local;
    } finally {
      RW_LOCK.writeLock().unlock();
    }

    LOGGER.debug("onStart event - finished pre loading all config interfaces");
  }

  private static List<Class<?>> getConfigInterfaces() {

    // Load the Jandex index (Quarkus automatically generates this file)
    InputStream indexStream =
        Thread.currentThread().getContextClassLoader().getResourceAsStream(JANDEX_LOCATION);

    if (indexStream == null) {
      LOGGER.error(
          "Unable to load Jandex index, will no pre-load configuration. Expected location={}",
          JANDEX_LOCATION);
      return List.of();
    }

    Index index = null;
    try {
      index = new IndexReader(indexStream).read();
    } catch (IOException e) {
      throw new RuntimeException("Error reading Jandex file to pre-load config", e);
    }

    // Create a DotName for the ConfigMapping annotation
    DotName configMappingDotName = DotName.createSimple(ConfigMapping.class.getName());

    // Query the index for all class-level annotations of @ConfigMapping,
    // map each annotation to its target ClassInfo, and filter by package name, interface, and then
    // get the Class<?>
    return index.getAnnotations(configMappingDotName).stream()
        .map(annotation -> annotation.target().asClass())
        .filter(ci -> ci.name().toString().startsWith(CONFIG_PACKAGE))
        .filter(ClassInfo::isInterface)
        .map(
            ci -> {
              try {
                return Class.forName(ci.name().toString());
              } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load class: " + ci.name(), e);
              }
            })
        .collect(java.util.stream.Collectors.toList());
  }
}
