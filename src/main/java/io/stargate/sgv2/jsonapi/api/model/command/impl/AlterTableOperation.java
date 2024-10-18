package io.stargate.sgv2.jsonapi.api.model.command.impl;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
  @JsonSubTypes.Type(value = AlterTableOperationImpl.AddColumns.class),
  @JsonSubTypes.Type(value = AlterTableOperationImpl.DropColumns.class),
  @JsonSubTypes.Type(value = AlterTableOperationImpl.AddVectorize.class),
  @JsonSubTypes.Type(value = AlterTableOperationImpl.DropVectorize.class)
})
public interface AlterTableOperation {}
