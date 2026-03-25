package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.grpc.exceptions.ServiceNotLiveException;
import io.grpc.ManagedChannel;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class SingleChannelManager extends ChannelManager {
    private final AtomicReference<ManagedChannel> channelReference = new AtomicReference<>();

    /**
     * Constructs a new gRPC connection factory using the provided host, port, and configuration.
     *
     * @param host gRPC service hostname or DNS target
     * @param port gRPC service port
     * @param configG configuration provider for channel and pool parameters
     * @param channelValidator method to determine if channel can successfully communicate with its associated server
     * @see ChannelManager
     */
    public SingleChannelManager(String host, int port, Configurator configG, Predicate<ManagedChannel> channelValidator) {
        super(host, port, configG, channelValidator);
        channelReference.set(create());
    }

    @Override
    public ManagedChannel create() {
        ManagedChannel channel = super.create();
        if (channelValidator.test(channel)) {
            return channel;
        }
        throw new ServiceNotLiveException("Unable to validate gRPC connection");
    }

    @Override
    public ManagedChannel acquire() {
        return channelReference.getAcquire();
    }

    @Override
    public void release(ManagedChannel channel) {
        /* No-op */
    }

    @Override
    public void shutdown(ManagedChannel channel) {
        channelReference.getAcquire().shutdownNow();
        channelReference.set(create());
    }

    @Override
    public void close() {
        channelReference.getAcquire().shutdownNow();
        channelReference.set(null);
    }
}
