package io.stargate.sgv2.jsonapi.testbench.testspec;

import java.util.List;

public record TestSpecMeta(String name, TestSpecKind kind, List<String> tags) {}
