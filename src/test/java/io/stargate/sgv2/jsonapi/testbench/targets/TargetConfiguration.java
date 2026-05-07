package io.stargate.sgv2.jsonapi.testbench.targets;

/**
 * Configuration record for a {@link Target}.
 * <p> --- </p>
 *
 * @param name Friendly name of the target
 * @param backend Backend name, such as astra or cassandra so we know how to run the lifecycle
 * @param connection Connection information for the target.
 */
public record TargetConfiguration(String name, String backend, ConnectionConfiguration connection) {}
