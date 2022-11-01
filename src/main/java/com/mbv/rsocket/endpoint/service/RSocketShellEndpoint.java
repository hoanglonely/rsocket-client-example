package com.mbv.rsocket.endpoint.service;

import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.metadata.WellKnownMimeType;
import io.rsocket.transport.ClientTransport;
import io.rsocket.transport.netty.client.WebsocketClientTransport;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.messaging.rsocket.RSocketStrategies;
import org.springframework.messaging.rsocket.annotation.support.RSocketMessageHandler;
import org.springframework.security.rsocket.metadata.BearerTokenAuthenticationEncoder;
import org.springframework.security.rsocket.metadata.BearerTokenMetadata;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.util.MimeType;
import org.springframework.util.MimeTypeUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;
import reactor.util.retry.Retry;

@Slf4j
@ShellComponent
public class RSocketShellEndpoint {

    private static final String backendUrl = "ws://10.2.100.198:7000";
    private static final String CLIENT_ID = UUID.randomUUID().toString();

    private final AtomicInteger pongsReceived = new AtomicInteger();

    private static final MimeType BEARER_AUTH = MimeTypeUtils.parseMimeType(WellKnownMimeType.MESSAGE_RSOCKET_AUTHENTICATION.getString());
    private static Disposable disposable;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private RSocketRequester rsocketRequester;
    private RSocketStrategies rsocketStrategies;
    private RSocketRequester.Builder rsocketRequesterBuilder;

    @Autowired
    public RSocketShellEndpoint(RSocketRequester.Builder builder,
                                @Qualifier("rSocketStrategies") RSocketStrategies strategies) {
        this.rsocketRequesterBuilder = builder;
        this.rsocketStrategies = strategies;
    }

    @ShellMethod("Login with your username and password.")
    public void loginjwt() {
        SocketAcceptor responder = RSocketMessageHandler.responder(rsocketStrategies, new ClientHandler());

        Algorithm algorithm = Algorithm.HMAC256("JAC1O17W1F3QB9E8B4B1MT6QKYOQB36V");

        HttpClient httpClient = HttpClient.create().baseUrl(backendUrl);

        String myToken = JWT.create()
                            .withJWTId(UUID.randomUUID().toString())
                            .withIssuer("rsocket-service-verification")
                            .withSubject("client")
                            .withExpiresAt(Date.from(Instant.now().plus(5, ChronoUnit.MINUTES)))
                            .withAudience("rsocket-service")
                            .withClaim("scope", "CLIENT")
                            .sign(algorithm);

        log.info("Token: {}", myToken);

        this.rsocketRequester = rsocketRequesterBuilder.setupRoute("open-connect")
                                                       .setupData(CLIENT_ID)
                                                       .setupMetadata(new BearerTokenMetadata(myToken),
                                                                      BEARER_AUTH) // (3)
                                                       .rsocketStrategies(builder -> builder.encoder(new BearerTokenAuthenticationEncoder())) // (4)
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

        this.disposable = Flux.interval(Duration.ofSeconds(5))
                              .flatMap(i -> rsocketRequester.route("pong")
                                                            .data("ping" + i)
                                                            .retrieveMono(String.class)
                                                            .doOnNext(this::logPongs))
                              .subscribe();
    }

    private void logPongs(String payload) {
        int received = pongsReceived.incrementAndGet();
        log.info("received " + payload + "(" + received + ") in Ping");
    }

    @PreDestroy
    @ShellMethod("Logout and close your connection")
    public void logout() {
        if (userIsLoggedIn()) {
            this.s();
            this.rsocketRequester.rsocketClient().dispose();
            log.info("Logged out.");
        }
    }

    @ShellMethod("Stop streaming messages from the server.")
    public void s() {
        if (null != disposable) {
            disposable.dispose();
        }
    }

    private boolean userIsLoggedIn() {
        if (null == this.rsocketRequester || this.rsocketRequester.rsocketClient().isDisposed()) {
            log.info("No connection. Did you login?");
            return false;
        }
        return true;
    }
}

@Slf4j
class ClientHandler {

    @MessageMapping("handle-transaction-verified")
    public void fireAndForget(TransactionVerifiedResponse status) {
        log.info("Received fire-and-forget request: {}", status.getStatus());
    }
}

@Data
@NoArgsConstructor
@AllArgsConstructor
class TransactionVerifiedResponse {
    private String sessionToken;
    private String status;
}