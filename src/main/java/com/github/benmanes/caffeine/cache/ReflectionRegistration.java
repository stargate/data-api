package com.github.benmanes.caffeine.cache;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection(targets = {com.github.benmanes.caffeine.cache.PSAMS.class})
public class ReflectionRegistration {
  // This class is used only for annotation processing during build
}
