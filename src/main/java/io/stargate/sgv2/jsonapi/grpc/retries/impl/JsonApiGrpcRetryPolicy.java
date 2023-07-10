package io.stargate.sgv2.jsonapi.grpc.retries.impl;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.ProtoUtils;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.sgv2.api.common.grpc.retries.GrpcRetryPredicate;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default gRPC retry policy used in the project. The policy defines retries when:
 *
 * <ol>
 *   <li>The received GRPC status code is <code>UNAVAILABLE</code>
 *   <li>The received GRPC status code is <code>DEADLINE_EXCEEDED</code>, but the metadata received
 *       contains the trailers set by the Bridge in case of a server-side read, write and CAS write
 *       timeouts.
 * </ol>
 *
 * Note that this class only defines the policy. Amount of retries and other retry properties are
 * defined by <code>stargate.grpc.retries</code> property group.
 */
@ApplicationScoped
public class JsonApiGrpcRetryPolicy implements GrpcRetryPredicate {
  private static final Logger logger = LoggerFactory.getLogger(JsonApiGrpcRetryPolicy.class);
  private static final Metadata.Key<QueryOuterClass.WriteTimeout> WRITE_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.WriteTimeout.getDefaultInstance());

  private static final Metadata.Key<QueryOuterClass.ReadTimeout> READ_TIMEOUT_KEY =
      ProtoUtils.keyForProto(QueryOuterClass.ReadTimeout.getDefaultInstance());

  /** {@inheritDoc} */
  @Override
  public boolean test(StatusRuntimeException e) {
    Status status = e.getStatus();
    Status.Code code = status.getCode();

    // always retry unavailable
    if (Objects.equals(code, Status.Code.UNAVAILABLE)) {
      return true;
    }

    // for timeouts, retry only server side timeouts
    if (Objects.equals(code, Status.Code.DEADLINE_EXCEEDED)) {
      return isValidServerSideTimeout(e.getTrailers());
    }

    // nothing else
    return false;
  }

  // ensure we retry only server side timeouts we want
  private boolean isValidServerSideTimeout(Metadata trailers) {
    // if we have trailers
    if (null != trailers) {
      // read, write and CAS write timeouts will include one of two trailers
      if (trailers.containsKey(READ_TIMEOUT_KEY) || trailers.containsKey(WRITE_TIMEOUT_KEY)) {
        logger.warn("DEADLINE_EXCEEDED with read/write timeout trailers, retrying");
        return true;
      }
    }

    // otherwise not
    return false;
  }
}
