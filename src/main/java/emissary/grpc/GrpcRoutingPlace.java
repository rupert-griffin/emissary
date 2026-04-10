package emissary.grpc;

import emissary.config.Configurator;
import emissary.grpc.channel.ChannelManager;
import emissary.grpc.channel.ChannelManagerFactory;
import emissary.grpc.future.CompletableFutureFinalizers;
import emissary.grpc.invoker.GrpcInvoker;
import emissary.grpc.retry.RetryHandler;
import emissary.place.ServiceProviderPlace;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractFutureStub;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Place for processing data using gRPC connections to external services. Supports multiple end-points with
 * <i>shared</i> configurations, where each endpoint is identified by a given target ID.
 * <p>
 * Configuration Keys:
 * <ul>
 * <li>{@code GRPC_HOST_{Target-ID}} - gRPC service hostname or DNS target, where {@code Target-ID} is the unique
 * identifier for the given host:port</li>
 * <li>{@code GRPC_PORT_{Target-ID}} - gRPC service port, where {@code Target-ID} is the unique identifier for the given
 * host:port</li>
 * <li>See {@link ChannelManager} for supported gRPC channel configuration keys and defaults.</li>
 * <li>See {@link RetryHandler} for supported retry configuration keys and defaults.</li>
 * </ul>
 */
public abstract class GrpcRoutingPlace extends ServiceProviderPlace implements IGrpcRoutingPlace {
    public static final Set<Status.Code> RETRY_GRPC_CODES = Set.of(
            Status.Code.UNAVAILABLE,
            Status.Code.DEADLINE_EXCEEDED,
            Status.Code.RESOURCE_EXHAUSTED);

    public static final String GRPC_HOST = "GRPC_HOST_";
    public static final String GRPC_PORT = "GRPC_PORT_";

    protected final Map<String, GrpcInvoker> invokerTable = new HashMap<>();

    protected GrpcRoutingPlace() throws IOException {
        super();
        configureGrpc();
    }

    protected GrpcRoutingPlace(String thePlaceLocation) throws IOException {
        super(thePlaceLocation);
        configureGrpc();
    }

    protected GrpcRoutingPlace(InputStream configStream) throws IOException {
        super(configStream);
        configureGrpc();
    }

    protected GrpcRoutingPlace(String configFile, String placeLocation) throws IOException {
        super(configFile, placeLocation);
        configureGrpc();
    }

    protected GrpcRoutingPlace(InputStream configStream, String placeLocation) throws IOException {
        super(configStream, placeLocation);
        configureGrpc();
    }

    protected GrpcRoutingPlace(String configFile, @Nullable String theDir, String thePlaceLocation) throws IOException {
        super(configFile, theDir, thePlaceLocation);
        configureGrpc();
    }

    protected GrpcRoutingPlace(InputStream configStream, @Nullable String theDir, String thePlaceLocation) throws IOException {
        super(configStream, theDir, thePlaceLocation);
        configureGrpc();
    }

    protected GrpcRoutingPlace(@Nullable Configurator configs) throws IOException {
        super(configs);
        configureGrpc();
    }

    private void configureGrpc() {
        Objects.requireNonNull(configG);

        Map<String, String> hosts = getHostnameConfigs();
        Map<String, Integer> ports = getPortNumberConfigs();

        if (!hosts.keySet().equals(ports.keySet())) {
            throw new IllegalArgumentException("gRPC hostname target-IDs do not match gRPC port number target-IDs");
        }

        Set<String> targetIds = hosts.keySet();
        if (targetIds.isEmpty()) {
            throw new NullPointerException(String.format(
                    "Missing required arguments: %s${Target-ID} and %s${Target-ID}", GRPC_HOST, GRPC_PORT));
        }

        RetryHandler retryHandler = new RetryHandler(configG, this.getPlaceName(), this::retryOnException);
        ChannelManagerFactory managerFactory = new ChannelManagerFactory(configG, this::validateConnection);
        for (String id : targetIds) {
            ChannelManager channelManager = managerFactory.build(hosts.get(id), ports.get(id));
            invokerTable.put(id, new GrpcInvoker(channelManager, retryHandler));
        }
    }

    protected Map<String, String> getHostnameConfigs() {
        return Objects.requireNonNull(configG).findStringMatchMap(GRPC_HOST, true);
    }

    protected Map<String, Integer> getPortNumberConfigs() {
        return Objects.requireNonNull(configG).findStringMatchMap(GRPC_PORT).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Integer.parseInt(entry.getValue())));
    }

    protected boolean retryOnException(Throwable t) {
        if (t instanceof StatusRuntimeException) {
            StatusRuntimeException e = (StatusRuntimeException) t;
            return RETRY_GRPC_CODES.contains(e.getStatus().getCode());
        }
        return false;
    }

    /**
     * Validates whether a given {@link ManagedChannel} is capable of successfully communicating with its associated gRPC
     * server.
     *
     * @param managedChannel the gRPC channel to validate
     * @return {@code true} if the channel is healthy and the server responds successfully, else {@code false}
     */
    protected abstract boolean validateConnection(ManagedChannel managedChannel);

    /**
     * Wrapper method for {@link GrpcInvoker#invoke(Function, BiFunction, GeneratedMessageV3)} that executes a unary gRPC
     * call to a given endpoint.
     *
     * @param targetId the identifier used in the configs for the given gRPC endpoint
     * @param stubFactory function that creates the appropriate gRPC stub from a {@link ManagedChannel}
     * @param callLogic function that performs the actual gRPC call using the stub and request
     * @param request the protobuf request message to send
     * @return the response returned by the gRPC call
     * @param <Q> the protobuf request type
     * @param <R> the protobuf response type
     * @param <S> the gRPC stub type
     */
    protected <Q extends GeneratedMessageV3, R extends GeneratedMessageV3, S extends AbstractBlockingStub<S>> R invokeGrpc(
            String targetId, Function<ManagedChannel, S> stubFactory, BiFunction<S, Q, R> callLogic, Q request) {
        return getInvoker(targetId).invoke(stubFactory, callLogic, request);
    }

    /**
     * Wrapper method for {@link GrpcInvoker#invokeAsync(Function, BiFunction, GeneratedMessageV3)} that executes a unary
     * gRPC call to a given endpoint and returns a {@link CompletableFuture future}.
     *
     * @param targetId the identifier used in the configs for the given gRPC endpoint
     * @param stubFactory function that creates the appropriate gRPC stub from a {@link ManagedChannel}
     * @param callLogic function that performs the actual gRPC call using the stub and request
     * @param request the protobuf request message to send
     * @return the future that waits for the response returned by the gRPC call
     * @param <Q> the protobuf request type
     * @param <R> the protobuf response type
     * @param <S> the gRPC stub type
     */
    protected <Q extends GeneratedMessageV3, R extends GeneratedMessageV3, S extends AbstractFutureStub<S>> CompletableFuture<R> invokeGrpcAsync(
            String targetId, Function<ManagedChannel, S> stubFactory, BiFunction<S, Q, ListenableFuture<R>> callLogic, Q request) {
        return getInvoker(targetId).invokeAsync(stubFactory, callLogic, request);
    }

    /**
     * Wrapper for {@link CompletableFutureFinalizers#awaitAllAndGet(Collection, Supplier, Function)}. If any future
     * completes exceptionally, the corresponding {@link CompletableFuture#join()} call will throw a
     * {@link RuntimeException}, and iteration will stop at that point.
     *
     * @param futures collection of futures representing asynchronous computations
     * @param factory creates the output collection instance
     * @return a new collection of the results returned by the asynchronous computations
     * @param <R> result type
     * @param <C> collection type
     */
    protected <R, C extends Collection<R>> C awaitAllAndGet(
            Collection<CompletableFuture<R>> futures, Supplier<? extends C> factory) {
        return CompletableFutureFinalizers.awaitAllAndGet(futures, factory, null);
    }

    /**
     * Wrapper for {@link CompletableFutureFinalizers#awaitAllAndGet(Collection, Supplier, Function)} with custom future
     * exception handling.
     *
     * @param futures collection of futures representing asynchronous computations
     * @param factory creates the output collection instance
     * @param exceptionally function for handling individual future exceptions, throws normally if {@code null}
     * @return a new collection of the results returned by the asynchronous computations
     * @param <R> result type
     * @param <C> collection type
     */
    protected <R, C extends Collection<R>> C awaitAllAndGet(
            Collection<CompletableFuture<R>> futures, Supplier<? extends C> factory, Function<Throwable, R> exceptionally) {
        return CompletableFutureFinalizers.awaitAllAndGet(futures, factory, exceptionally);
    }

    /**
     * Wrapper for {@link CompletableFutureFinalizers#awaitAllAndGet(Map, Supplier, Function)}. If any future completes
     * exceptionally, the corresponding {@link CompletableFuture#join()} call will throw a {@link RuntimeException}, and
     * iteration will stop at that point.
     *
     * @param futures map of keys to futures representing asynchronous computations
     * @param factory creates the output map instance
     * @return a new map of keys to the results returned by the asynchronous computations
     * @param <K> key type
     * @param <R> result type
     * @param <M> map type
     */
    protected <K, R, M extends Map<K, R>> M awaitAllAndGet(
            Map<K, CompletableFuture<R>> futures, Supplier<? extends M> factory) {
        return CompletableFutureFinalizers.awaitAllAndGet(futures, factory, null);
    }

    /**
     * Wrapper for {@link CompletableFutureFinalizers#awaitAllAndGet(Map, Supplier, Function)} with custom future exception
     * handling.
     *
     * @param futures map of keys to futures representing asynchronous computations
     * @param factory creates the output map instance
     * @param exceptionally function for handling individual future exceptions, throws normally if {@code null}
     * @return a new map of keys to the results returned by the asynchronous computations
     * @param <K> key type
     * @param <R> result type
     * @param <M> map type
     */
    protected <K, R, M extends Map<K, R>> M awaitAllAndGet(
            Map<K, CompletableFuture<R>> futures, Supplier<? extends M> factory, Function<Throwable, R> exceptionally) {
        return CompletableFutureFinalizers.awaitAllAndGet(futures, factory, exceptionally);
    }

    public String getHostname(String targetId) {
        return getInvoker(targetId).getHost();
    }

    public int getPortNumber(String targetId) {
        return getInvoker(targetId).getPort();
    }

    private GrpcInvoker getInvoker(String targetId) {
        if (invokerTable.containsKey(targetId)) {
            return invokerTable.get(targetId);
        }
        throw new IllegalArgumentException(String.format("Target-ID %s was never configured", targetId));
    }

    @Override
    public void shutDown() {
        super.shutDown();
        invokerTable.values().forEach(GrpcInvoker::close);
    }
}
