package io.stargate.sgv2.jsonapi;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * Keeps external list of classes need to be registered for reflection.
 *
 * @see <a
 *     href='https://quarkus.io/guides/writing-native-applications-tips#registering-for-reflection'>https://quarkus.io/guides/writing-native-applications-tips#registering-for-reflection</a>
 * @see <a
 *     href='https://quarkus.io/guides/cache#going-native'>https://quarkus.io/guides/cache#going-native</a>
 */
@RegisterForReflection(classNames = {"com.github.benmanes.caffeine.cache.PSAMW"})
public class ReflectionConfiguration {}
