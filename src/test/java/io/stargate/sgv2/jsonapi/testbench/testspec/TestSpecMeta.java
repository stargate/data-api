package io.stargate.sgv2.jsonapi.testbench.testspec;

import java.util.List;

/**
 * Common metadata for any object of a {@link TestSpec}
 * @param name Friendly name for, such as "vectorize-header-workflow"
 * @param kind {@link TestSpecKind} describing the kind.
 * @param tags Optional tags that can be used for filtering etc.
 */
public record TestSpecMeta(String name, TestSpecKind kind, List<String> tags) {

    public TestSpecMeta{
        tags = tags == null ? List.of() : tags;
    }
}
