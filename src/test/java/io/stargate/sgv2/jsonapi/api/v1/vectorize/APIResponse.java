package io.stargate.sgv2.jsonapi.api.v1.vectorize;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.restassured.response.ValidatableResponse;

public record APIResponse (APIRequest apiRequest,
                           ValidatableResponse validatableResponse) {
}
