package io.stargate.sgv2.jsonapi;

import io.quarkus.grpc.GrpcService;
import io.stargate.sgv2.api.common.grpc.qualifier.Retriable;
import jakarta.annotation.Priority;
import jakarta.enterprise.inject.Alternative;
import jakarta.inject.Singleton;
import jakarta.ws.rs.Produces;

@Alternative
@Priority(1)
@GrpcService
@Singleton
public class JsonapiRetriableStargateBridgeProvider {

  @Produces
  @Retriable
  io.stargate.sgv2.api.common.grpc.RetriableStargateBridge retriableStargateBridge() {
    return null;
  }
}
