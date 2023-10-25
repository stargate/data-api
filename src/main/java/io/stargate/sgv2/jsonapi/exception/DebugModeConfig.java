package io.stargate.sgv2.jsonapi.exception;

//import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;


//@StaticInitSafe
@ConfigMapping(prefix = "stargate.haha")
public interface DebugModeConfig {

  @WithDefault("false")
  boolean enabled();
}
