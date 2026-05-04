package io.stargate.sgv2.jsonapi.testbench.messaging;

import io.restassured.response.ValidatableResponse;

public record APIResponse(APIRequest apiRequest, ValidatableResponse validatable) {}
