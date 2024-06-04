package io.stargate.sgv2.jsonapi.api.security.challenge.impl;

import io.quarkus.vertx.http.runtime.security.ChallengeData;
import io.quarkus.vertx.http.runtime.security.HttpAuthenticationMechanism;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.util.function.BiFunction;

/**
 * Enables definition of the custom challenge sender in the application.
 *
 * @see HttpAuthenticationMechanism#sendChallenge(RoutingContext)
 */
public interface ChallengeSender extends BiFunction<RoutingContext, ChallengeData, Uni<Boolean>> {}
