package io.stargate.sgv2.jsonapi.exception.playing;

// TODO has all the fields we want to return in the error object in the request
public record CommandResponseError (String errorFamily, String code, String title, String detail){
}
