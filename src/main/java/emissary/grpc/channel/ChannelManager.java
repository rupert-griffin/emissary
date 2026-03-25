package emissary.grpc.channel;

import emissary.config.Configurator;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Abstract base class for managing a pool of gRPC {@link io.grpc.ManagedChannel} connections.
 * <p>
 * Configuration Keys:
 * <ul>
 * <li>{@code GRPC_KEEP_ALIVE_MILLIS} - Time to wait before sending a ping on idle, default={@code 60000L}</li>
 * <li>{@code GRPC_KEEP_ALIVE_TIMEOUT_MILLIS} - Timeout for receiving ping ACKs, default={@code 30000L}</li>
 * <li>{@code GRPC_KEEP_ALIVE_WITHOUT_CALLS} - Send pings even when no RPCs are active if {@code true},
 * default={@code false}</li>
 * <li>{@code GRPC_LOAD_BALANCING_POLICY} - gRPC load balancing policy, default={@code "round_robin"}</li>
 * <li>{@code GRPC_MAX_INBOUND_MESSAGE_BYTE_SIZE} - Max inbound gRPC message size, default={@code 4194304}</li>
 * <li>{@code GRPC_MAX_INBOUND_METADATA_BYTE_SIZE} - Max inbound gRPC metadata size, default={@code 8192}</li>
 */
public abstract class ChannelManager {
    public static final String GRPC_KEEP_ALIVE_MILLIS = "GRPC_KEEP_ALIVE_MILLIS";
    public static final String GRPC_KEEP_ALIVE_TIMEOUT_MILLIS = "GRPC_KEEP_ALIVE_TIMEOUT_MILLIS";
    public static final String GRPC_KEEP_ALIVE_WITHOUT_CALLS = "GRPC_KEEP_ALIVE_WITHOUT_CALLS";
    public static final String GRPC_LOAD_BALANCING_POLICY = "GRPC_LOAD_BALANCING_POLICY";
    public static final String GRPC_MAX_INBOUND_MESSAGE_BYTE_SIZE = "GRPC_MAX_INBOUND_MESSAGE_BYTE_SIZE";
    public static final String GRPC_MAX_INBOUND_METADATA_BYTE_SIZE = "GRPC_MAX_INBOUND_METADATA_BYTE_SIZE";

    protected final Logger logger;

    protected final Predicate<ManagedChannel> channelValidator;

    protected final String host;
    protected final int port;
    protected final String target;
    protected final long keepAliveMillis;
    protected final long keepAliveTimeoutMillis;
    protected final boolean keepAliveWithoutCalls;
    protected final int maxInboundMessageByteSize;
    protected final int maxInboundMetadataByteSize;
    protected final String loadBalancingPolicy;

    /**
     * Constructs a new gRPC connection manager using the provided host, port, and configuration. Initializes gRPC
     * channel properties from the given configuration source.
     *
     * @param host gRPC service hostname or DNS target
     * @param port gRPC service port
     * @param configG configuration provider for channel and pool parameters
     * @param channelValidator method to determine if channel can successfully communicate with its associated server
     * @see ChannelManager
     * @see <a href="https://docs.microsoft.com/en-us/aspnet/core/grpc/performance?view=aspnetcore-5.0">Source</a> for 
     * default gRPC configurations.
     */
    protected ChannelManager(String host, int port, Configurator configG, Predicate<ManagedChannel> channelValidator) {
        this.logger = LoggerFactory.getLogger(this.getClass().getName());

        this.host = host;
        this.port = port;
        this.target = host + ":" + port; // target may be a host or dns service

        this.channelValidator = channelValidator;

        // How often (in milliseconds) to send pings when the connection is idle
        this.keepAliveMillis = configG.findLongEntry(GRPC_KEEP_ALIVE_MILLIS, 60000L);

        // Time to wait (in milliseconds) for a ping ACK before closing the connection
        this.keepAliveTimeoutMillis = configG.findLongEntry(GRPC_KEEP_ALIVE_TIMEOUT_MILLIS, 30000L);

        // Whether to send pings when no RPCs are active
        // Note: Seme gRPC services have this set to false and will be noisy if not adjusted
        this.keepAliveWithoutCalls = configG.findBooleanEntry(GRPC_KEEP_ALIVE_WITHOUT_CALLS, false);

        // Specifies how the client chooses between multiple backend addresses
        // e.g. "pick_first" uses the first address only, "round_robin" cycles through all of them for client-side balancing
        this.loadBalancingPolicy = LoadBalancingPolicy.getPolicyName(
                configG.findStringEntry(GRPC_LOAD_BALANCING_POLICY), LoadBalancingPolicy.ROUND_ROBIN);

        // Max size (in bytes) for incoming messages and message metadata from the server
        this.maxInboundMessageByteSize = configG.findIntEntry(GRPC_MAX_INBOUND_MESSAGE_BYTE_SIZE, 4 << 20); // 4 MiB
        this.maxInboundMetadataByteSize = configG.findIntEntry(GRPC_MAX_INBOUND_METADATA_BYTE_SIZE, 8 << 10); // 8 KiB
    }

    /**
     * Creates a new {@link ManagedChannel} instance configured with the current factory settings. Called internally by the
     * object pool during channel instantiation.
     *
     * @return a new gRPC channel
     */
    protected ManagedChannel create() {
        return ManagedChannelBuilder.forTarget(this.target)
                .keepAliveTime(this.keepAliveMillis, TimeUnit.MILLISECONDS)
                .keepAliveTimeout(this.keepAliveTimeoutMillis, TimeUnit.MILLISECONDS)
                .keepAliveWithoutCalls(this.keepAliveWithoutCalls)
                .defaultLoadBalancingPolicy(this.loadBalancingPolicy)
                .maxInboundMessageSize(this.maxInboundMessageByteSize)
                .maxInboundMetadataSize(this.maxInboundMetadataByteSize)
                .usePlaintext().build();
    }

    public abstract ManagedChannel acquire();

    public abstract void release(ManagedChannel channel);

    public abstract void shutdown(ManagedChannel channel);

    public abstract void close();

    public enum LoadBalancingPolicy {
        ROUND_ROBIN, PICK_FIRST;

        public static LoadBalancingPolicy getPolicy(String input, LoadBalancingPolicy defaultPolicy) {
            if (input == null) {
                return defaultPolicy;
            }
            return LoadBalancingPolicy.valueOf(input.toUpperCase(Locale.ROOT));
        }

        public static String getPolicyName(String input, LoadBalancingPolicy defaultPolicy) {
            return LoadBalancingPolicy
                    .getPolicy(input, defaultPolicy)
                    .name()
                    .toLowerCase(Locale.ROOT);
        }
    }
}
