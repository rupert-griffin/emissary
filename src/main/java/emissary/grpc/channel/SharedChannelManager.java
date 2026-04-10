package emissary.grpc.channel;

import emissary.config.Configurator;

import io.grpc.ManagedChannel;

import java.util.concurrent.atomic.AtomicReference;

public class SharedChannelManager extends ChannelManager {
    private final AtomicReference<ManagedChannel> channelReference = new AtomicReference<>();

    /**
     * Constructs a new gRPC connection factory using the provided host, port, and configuration.
     *
     * @param host gRPC service hostname or DNS target
     * @param port gRPC service port
     * @param configG configuration provider for channel parameters
     * @param channelValidator method to determine if channel can successfully communicate with its associated server
     * @see ChannelManager
     */
    public SharedChannelManager(String host, int port, Configurator configG, ChannelValidator channelValidator) {
        super(host, port, configG, channelValidator);
    }

    @Override
    public ManagedChannel acquire() {
        ManagedChannel channel = channelReference.getAcquire();
        if (channel == null || channel.isShutdown()) {
            channel = create();
            channelReference.set(channel);
        }
        return channel;
    }

    @Override
    public void release(ManagedChannel channel) {
        /* No-op */
    }

    @Override
    public void shutdown(ManagedChannel channel) {
        channel.shutdownNow();
    }

    @Override
    public void close() {
        ManagedChannel channel = channelReference.getAcquire();
        if (channel != null) {
            if (!channel.isShutdown()) {
                shutdown(channel);
            }
            channelReference.set(null);
        }
    }
}
