package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import java.util.List;

public record ITMetadata(String name, String kind,
                         List<String> tags) {}
