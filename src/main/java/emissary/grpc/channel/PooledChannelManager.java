package emissary.grpc.channel;

import emissary.config.Configurator;
import io.grpc.ManagedChannel;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Manages a pool of {@link ManagedChannel ManagedChannels} without enforcing exclusive use. gRPC channels can handle
 * many simultaneous connections, allowing multiple Emissary threads to share one instance. For most use-cases, a pool
 * size of 1 should be sufficient, though it can be configured higher as necessary.
 */
public class PooledChannelManager extends ChannelManager {
    public static final String POOL_SIZE = "GRPC_CHANNEL_POOL_SIZE";
    public static final String POOL_SIZE_PREFIX = POOL_SIZE + "_";

    private final AtomicInteger counter = new AtomicInteger(0);
    private final AtomicReferenceArray<ManagedChannel> channels;

    /**
     * Constructs a new gRPC connection factory using the provided host, port, and configuration.
     *
     * @param host gRPC service hostname or DNS target
     * @param port gRPC service port
     * @param configG configuration provider for channel parameters
     * @param channelValidator method to determine if channel can successfully communicate with its associated server
     * @see ChannelManager
     */
    public PooledChannelManager(String id, String host, int port, Configurator configG, ChannelValidator channelValidator) {
        super(id, host, port, configG, channelValidator);
        int poolSize = findPoolSizeEntry(configG);

        channels = new AtomicReferenceArray<>(poolSize);
        for (int i = 0; i < poolSize; i++) {
            channels.set(i, create(i));
        }
    }

    private int findPoolSizeEntry(Configurator configG) {
        Map<String, String> poolSizeMap = configG.findStringMatchMap(POOL_SIZE_PREFIX);
        if (poolSizeMap.containsKey(id)) {
            return Integer.parseInt(poolSizeMap.get(id));
        }
        return configG.findIntEntry(POOL_SIZE, 1);
    }

    private ManagedChannel create(int index) {
        return create(Map.of("index", index));
    }

    @Override
    public ManagedChannel acquire() {
        int index = Math.floorMod(counter.getAndIncrement(), channels.length());
        while (true) {
            ManagedChannel channel = channels.get(index);
            if (channel != null && !channel.isShutdown() && !channel.isTerminated()) {
                return channel;
            }
            logger.warn("Replacing dead ManagedChannel at index {}", index);
            ManagedChannel replacement = create(index);
            if (channels.compareAndSet(index, channel, replacement)) {
                if (channel != null) {
                    channel.shutdownNow();
                }
                return replacement;
            }
            replacement.shutdownNow();
        }
    }

    @Override
    public void shutdown(ManagedChannel channel) {
        channel.shutdownNow();
    }

    @Override
    public void close() {
        for (int i = 0; i < channels.length(); i++) {
            ManagedChannel channel = channels.get(i);
            if (channel != null) {
                if (!channel.isShutdown()) {
                    shutdown(channel);
                }
                channels.set(i, null);
            }
        }
    }
}
