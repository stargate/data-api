package io.stargate.sgv2.jsonapi.api.model.command.clause.sort;

import jakarta.validation.constraints.NotBlank;

public record SortExpression(

    // TODO correct typing for the path, we could use some kind of a common class
    //  we also need a validation of a correct path here
    @NotBlank String path,

    // this can be modeled in different ways, would this be enough for now
    boolean ascending) {}
