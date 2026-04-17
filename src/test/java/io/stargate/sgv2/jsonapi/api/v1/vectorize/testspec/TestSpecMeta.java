package io.stargate.sgv2.jsonapi.api.v1.vectorize.testspec;

import java.util.List;

public record TestSpecMeta(String name, TestSpecKind kind, List<String> tags) {}
