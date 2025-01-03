package io.stargate.sgv2.jsonapi.config.constants;

import io.stargate.sgv2.jsonapi.util.defaults.DefaultBoolean;
import io.stargate.sgv2.jsonapi.util.defaults.Defaults;

/** Default values applied to table descriptions. */
public interface TableDescDefaults {

  // Defaults to apply to the value from {@link RegularIndexDesc}
  interface RegularIndexDescDefaults {
    DefaultBoolean ASCII = Defaults.of(false);
    DefaultBoolean CASE_SENSITIVE = Defaults.of(true);
    DefaultBoolean NORMALIZE = Defaults.of(false);
  }
}
