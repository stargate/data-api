package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import io.stargate.sgv2.jsonapi.api.v1.vectorize.targets.Connection;

public record TargetConfiguration(String name, String backend, Connection connection) {
}
