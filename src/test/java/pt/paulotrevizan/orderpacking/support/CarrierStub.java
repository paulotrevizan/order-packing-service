package pt.paulotrevizan.orderpacking.support;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.ResponseDefinitionBuilder;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;

/**
 * The carrier (HLD 7.3) as WireMock. Every order gets a label unless a test stubs a different
 * answer for its order id; the request journal is how scenarios count carrier calls (I2).
 */
public class CarrierStub implements AutoCloseable {

  public static final String LABEL_ID = "LBL-778";

  private static final String LABELS_PATH = "/labels";
  private static final int DEFAULT_PRIORITY = 10;
  private static final int ORDER_PRIORITY = 1;

  private final WireMockServer server;

  public CarrierStub() {
    // A fixed port, so the carrier can be stopped and started again at the same address.
    this.server = new WireMockServer(WireMockConfiguration.options().port(freePort()));
    server.start();
    server.stubFor(
        post(urlEqualTo(LABELS_PATH)).atPriority(DEFAULT_PRIORITY).willReturn(created()));
  }

  public String baseUrl() {
    return "http://localhost:" + server.port();
  }

  public void rejects(String orderId) {
    server.stubFor(forOrder(orderId).willReturn(aResponse().withStatus(400)));
  }

  public void failsWithServerError(String orderId) {
    server.stubFor(forOrder(orderId).willReturn(aResponse().withStatus(500)));
  }

  public void respondsAfter(String orderId, Duration delay) {
    server.stubFor(forOrder(orderId).willReturn(created().withFixedDelay((int) delay.toMillis())));
  }

  /** Nothing listens on the port while stopped: connections are refused. */
  public void stop() {
    server.stop();
  }

  public void start() {
    server.start();
  }

  public List<LoggedRequest> requests(String orderId) {
    return server.findAll(
        postRequestedFor(urlEqualTo(LABELS_PATH))
            .withRequestBody(matchingJsonPath("$.orderId", equalTo(orderId))));
  }

  @Override
  public void close() {
    server.stop();
  }

  private static MappingBuilder forOrder(String orderId) {
    return post(urlEqualTo(LABELS_PATH))
        .atPriority(ORDER_PRIORITY)
        .withRequestBody(matchingJsonPath("$.orderId", equalTo(orderId)));
  }

  private static ResponseDefinitionBuilder created() {
    return aResponse()
        .withStatus(201)
        .withHeader("Content-Type", "application/json")
        .withBody(
            """
                {"labelId": "%s"}
                """
                .formatted(LABEL_ID));
  }

  private static int freePort() {
    try (var socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException exception) {
      throw new UncheckedIOException(exception);
    }
  }
}
