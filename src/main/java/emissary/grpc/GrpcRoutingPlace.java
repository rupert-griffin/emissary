package emissary.grpc;

import emissary.config.Configurator;
import emissary.grpc.channel.ChannelManager;
import emissary.grpc.channel.PooledChannelManager;
import emissary.grpc.exceptions.PoolException;
import emissary.grpc.exceptions.ServiceException;
import emissary.grpc.exceptions.ServiceNotAvailableException;
import emissary.grpc.retry.RetryHandler;
import emissary.place.ServiceProviderPlace;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractFutureStub;
import jakarta.annotation.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
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
    public static final String GRPC_HOST = "GRPC_HOST_";
    public static final String GRPC_PORT = "GRPC_PORT_";

    protected RetryHandler retryHandler;
    protected final Map<String, String> hostnameTable = new HashMap<>();
    protected final Map<String, Integer> portNumberTable = new HashMap<>();
    protected final Map<String, ChannelManager> channelManagerTable = new HashMap<>();

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

    protected Map<String, String> getHostnameConfigs() {
        return Objects.requireNonNull(configG).findStringMatchMap(GRPC_HOST, true);
    }

    protected Map<String, Integer> getPortNumberConfigs() {
        return Objects.requireNonNull(configG).findStringMatchMap(GRPC_PORT).entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Integer.parseInt(entry.getValue())));
    }

    private void configureGrpc() {
        if (configG == null) {
            throw new IllegalStateException("gRPC configurations not found for " + this.getPlaceName());
        }

        hostnameTable.putAll(getHostnameConfigs());
        portNumberTable.putAll(getPortNumberConfigs());

        if (!hostnameTable.keySet().equals(portNumberTable.keySet())) {
            throw new IllegalArgumentException("gRPC hostname target-IDs do not match gRPC port number target-IDs");
        }

        if (hostnameTable.isEmpty()) {
            throw new NullPointerException(String.format(
                    "Missing required arguments: %s${Target-ID} and %s${Target-ID}", GRPC_HOST, GRPC_PORT));
        }

        hostnameTable.forEach((id, host) -> channelManagerTable.put(id, new PooledChannelManager(
                host, portNumberTable.get(id), configG, this::validateConnection)));

        retryHandler = new RetryHandler(configG, this.getPlaceName());
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
     * Executes a unary gRPC call to a given endpoint using a {@code BlockingStub}. If the gRPC connection fails due to a
     * {@link PoolException} or a {@link ServiceNotAvailableException}, the call will be tried again per the configurations
     * set using {@link RetryHandler}. All other Exceptions are thrown on the spot. Will also throw an Exception once max
     * attempts have been reached.
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

        return retryHandler.execute(() -> {
            ChannelManager channelManager = getChannelManager(targetId);
            ManagedChannel channel = channelManager.acquire();
            R response = null;
            try {
                S stub = stubFactory.apply(channel);
                response = callLogic.apply(stub, request);
                channelManager.release(channel);
            } catch (StatusRuntimeException e) {
                channelManager.shutdown(channel);
                ServiceException.handleGrpcStatusRuntimeException(e);
            } catch (RuntimeException e) {
                channelManager.shutdown(channel);
                throw e;
            }
            return response;
        });
    }

    /**
     * Executes multiple unary gRPC calls to a given endpoint in parallel using a shared {@link AbstractFutureStub}.
     * <p>
     * TODO: Determine channel handling strategy when some calls succeed and others fail <br>
     * TODO: Clarify expected blocking behavior for response collection
     *
     * @param targetId the identifier used in the configs for the given gRPC endpoint
     * @param stubFactory function that creates the appropriate {@code FutureStub} from a {@link ManagedChannel}
     * @param callLogic function that maps a stub and request to a {@link ListenableFuture}
     * @param requestList list of protobuf request messages to be sent
     * @return list of gRPC responses in the same order as {@code requestList}
     * @param <Q> the protobuf request type
     * @param <R> the protobuf response type
     * @param <S> the gRPC stub type
     */
    protected <Q extends GeneratedMessageV3, R extends GeneratedMessageV3, S extends AbstractFutureStub<S>> List<R> invokeBatchedGrpc(
            String targetId, Function<ManagedChannel, S> stubFactory,
            BiFunction<S, Q, ListenableFuture<R>> callLogic, List<Q> requestList) {

        throw new UnsupportedOperationException("Not yet implemented");
    }

    private ChannelManager getChannelManager(String targetId) {
        return tableLookup(channelManagerTable, targetId);
    }

    public String getHostname(String targetId) {
        return tableLookup(hostnameTable, targetId);
    }

    public int getPortNumber(String targetId) {
        return tableLookup(portNumberTable, targetId);
    }

    protected <T> T tableLookup(Map<String, T> table, String targetId) {
        if (table.containsKey(targetId)) {
            return table.get(targetId);
        }
        throw new IllegalArgumentException(String.format("Target-ID %s was never configured", targetId));
    }

    @Override
    public void shutDown() {
        channelManagerTable.values().forEach(ChannelManager::close);
        super.shutDown();
    }
}
