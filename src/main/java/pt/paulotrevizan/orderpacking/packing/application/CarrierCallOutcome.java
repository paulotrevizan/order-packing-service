package pt.paulotrevizan.orderpacking.packing.application;

import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.util.Objects;
import java.util.stream.Stream;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestClientResponseException;

enum CarrierCallOutcome {
  CREATED,
  REFUSED, // no label was created (safe to call again later)
  REJECTED,
  UNKNOWN; // a label may exist (never call again automatically)

  static CarrierCallOutcome of(RestClientException exception) {
    return switch (exception) {
      case RestClientResponseException response
          when response.getStatusCode().isSameCodeAs(HttpStatus.TOO_MANY_REQUESTS) ->
          REFUSED;
      case RestClientResponseException response when response.getStatusCode().is4xxClientError() ->
          REJECTED;
      case RestClientResponseException _ -> UNKNOWN;
      default -> neverConnected(exception) ? REFUSED : UNKNOWN;
    };
  }

  // the connection never opened, so the carrier cannot have created a label.
  private static boolean neverConnected(RestClientException exception) {
    return Stream.iterate(exception, Objects::nonNull, Throwable::getCause)
        .anyMatch(
            cause ->
                cause instanceof ConnectException || cause instanceof HttpConnectTimeoutException);
  }
}
