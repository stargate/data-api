package io.stargate.sgv2.jsonapi.service;

/** Hack / temp class to have run time checking for if Tables are supported */
public final class FeatureFlags {

  public static final boolean TABLES_SUPPORTED = Boolean.getBoolean("stargate.tables.supported");
}
