package io.stargate.sgv2.jsonapi.testbench.testspec;

import io.stargate.sgv2.jsonapi.testbench.targets.Connection;

/**
 * A target the test bench should connect to and run the tests
 * @param name Friendly name of the target
 * @param backend Backend name, such as astra or cassandra so we know how to run the lifecycle
 * @param connection Connection information for the target.
 */
public record TargetConfiguration(String name, String backend, Connection connection) {}
