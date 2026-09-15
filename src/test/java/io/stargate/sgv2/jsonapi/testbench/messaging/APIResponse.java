package io.stargate.sgv2.jsonapi.testbench.messaging;

import io.restassured.response.ValidatableResponse;

/** Basic holder for the response, so we can tie it back to the request that created it */
public record APIResponse(APIRequest apiRequest, ValidatableResponse validatable) {}
