package io.stargate.sgv2.jsonapi.api.v1.vectorize.assertions;

import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;

public record ITAssertion(
    String bodyPath,
    Matcher<?> matcher
) {}