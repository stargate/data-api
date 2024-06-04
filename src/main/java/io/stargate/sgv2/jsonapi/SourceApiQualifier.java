package io.stargate.sgv2.jsonapi;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;

@Documented
@Retention(RUNTIME)
@Qualifier
public @interface SourceApiQualifier {}
