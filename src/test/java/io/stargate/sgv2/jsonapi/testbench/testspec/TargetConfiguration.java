package io.stargate.sgv2.jsonapi.testbench.testspec;

import io.stargate.sgv2.jsonapi.testbench.targets.Connection;

public record TargetConfiguration(String name, String backend, Connection connection) {}
