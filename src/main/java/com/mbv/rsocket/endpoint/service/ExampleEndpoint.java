package com.mbv.rsocket.endpoint.service;

import com.mbv.rsocket.model.Message;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.SimpleAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.UsernamePasswordMetadata;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

@Slf4j
@ShellComponent
public class ExampleEndpoint {

    private static final String CLIENT = "Client";
    private static final String REQUEST = "Request";
    private static final String FIRE_AND_FORGET = "Fire-And-Forget";
    private static final String STREAM = "Stream";
    private static final String backendUrl = "ws://10.2.100.198:7000";
    private static final String CLIENT_ID = UUID.randomUUID().toString();
    private static final MimeType SIMPLE_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private static Disposable disposable;
    private RSocketRequester rsocketRequester;

    private RSocketStrategies rsocketStrategies;
    private RSocketRequester.Builder rsocketRequesterBuilder;

    @Autowired
    public ExampleEndpoint(RSocketRequester.Builder builder,
                           @Qualifier("rSocketStrategies") RSocketStrategies strategies) {
        this.rsocketRequesterBuilder = builder;
        this.rsocketStrategies = strategies;
    }

    @ShellMethod("Login with your username and password.")
    public void login(String username, String password) {
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new ClientHandler());

        HttpClient httpClient = HttpClient.create().baseUrl(backendUrl);

        UsernamePasswordMetadata user = new UsernamePasswordMetadata(username, password); // (2)

        this.rsocketRequester = rsocketRequesterBuilder.setupRoute("open-connect")
                                                       .setupData(CLIENT_ID)
                                                       .setupMetadata(user, SIMPLE_AUTH) // (3)
                                                       .rsocketStrategies(builder -> builder.encoder(new SimpleAuthenticationEncoder())) // (4)
                                                       .rsocketConnector(connector -> connector.acceptor(responder)
                                                                                               .reconnect(Retry.fixedDelay(
                                                                                                       Integer.MAX_VALUE,
                                                                                                       Duration.ofSeconds(
                                                                                                               5))))
                                                       .transport(() -> {
                                                           ClientTransport t = WebsocketClientTransport.create(
                                                                   httpClient,
                                                                   "/rsocket");
                                                           return t.connect().doOnNext(connection -> {
                                                               log.warn("Connected");
                                                               connected.set(true);
                                                           }).doOnError(error -> {
                                                               log.warn("Disconnected");
                                                               connected.set(false);
                                                           });
                                                       });

        // ...connection handling code omitted. See the sample for details.

        this.rsocketRequester.rsocketClient()
                             .source()
                             .flatMap(RSocket::onClose)
                             .repeat()
                             .retryWhen(Retry.backoff(10, Duration.ofSeconds(1)))
                             .subscribe();

        this.disposable = Flux.interval(Duration.ofSeconds(10))
                              .log()
                              .onBackpressureDrop()
                              .map(tick -> "Telemetry from " + CLIENT_ID + " tick " + tick)
                              .as(flux -> rsocketRequester.route("/backend/telemetry")
                                                          .data(flux)
                                                          .retrieveFlux(Void.class))
                              .subscribe();
    }

    @ShellMethod("Stream some settings to the server. Stream of responses will be printed.")
    public void channel() {

        Mono<Duration> setting1 = Mono.just(Duration.ofSeconds(1));
        Mono<Duration> setting2 = Mono.just(Duration.ofSeconds(3)).delayElement(Duration.ofSeconds(5));
        Mono<Duration> setting3 = Mono.just(Duration.ofSeconds(5)).delayElement(Duration.ofSeconds(15));

        Flux<Duration> settings = Flux.concat(setting1, setting2, setting3)
                                      .doOnNext(d -> log.info("\nSending setting for {}-second interval.\n",
                                                              d.getSeconds()));

        disposable = this.rsocketRequester.route("channel")
                                          .data(settings)
                                          .retrieveFlux(Message.class)
                                          .subscribe(message -> log.info("Received: {} \n(Type 's' to stop.)",
                                                                         message));

    }

    @ShellMethod("Send one request. No response will be returned.")
    public void fireAndForget() throws InterruptedException {
        log.info("\nFire-And-Forget. Sending one request. Expect no response (check server log)...");
        this.rsocketRequester.route("fire-and-forget").data(new Message(CLIENT, FIRE_AND_FORGET)).send().block();
    }

    @ShellMethod("Send one request. Many responses (stream) will be printed.")
    public void stream() {
        log.info("\nRequest-Stream. Sending one request. Waiting for unlimited responses (Stop process to quit)...");
        this.disposable = this.rsocketRequester.route("stream")
                                               .data(new Message(CLIENT, STREAM))
                                               .retrieveFlux(Message.class)
                                               .subscribe(er -> log.info("Response received: {}", er));
    }

    @ShellMethod("Send one request. One response will be printed.")
    public void requestResponse() throws InterruptedException {
        log.info("\nSending one request. Waiting for one response...");
        Message message = this.rsocketRequester.route("request-response")
                                               .data(new Message(CLIENT, REQUEST))
                                               .retrieveMono(Message.class)
                                               .block();
        log.info("\nResponse was: {}", message);
    }

    @ShellMethod("Send one request. One response will be printed.")
    public void helloSecureAdminOnly() throws InterruptedException {
        log.info("\nSending one request hello.secure.adminonly. Waiting for one response...");
        String message = this.rsocketRequester.route("hello.secure.adminonly")
                                              .data(REQUEST)
                                              .retrieveMono(String.class)
                                              .block();
        log.info("\nResponse was: {}", message);
    }
}
