package io.stargate.sgv2.jsonapi.config.constants;

/** Default values applied to table descriptions. */
public interface TableDescDefaults {

  // Defaults to apply to the value from {@link RegularIndexDesc}
  interface RegularIndexDescDefaults {
    boolean ASCII = false;
    boolean CASE_SENSITIVE = true;
    boolean NORMALIZE = false;
  }
}
