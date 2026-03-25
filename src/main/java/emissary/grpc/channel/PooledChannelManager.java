package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.grpc.exceptions.PoolException;
import io.grpc.ManagedChannel;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.BaseObjectPoolConfig;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.time.Duration;
import java.util.Locale;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Manages a pool of gRPC {@link ManagedChannel} connections.
 * <p>
 * Configuration Keys:
 * <ul>
 * <li>{@code GRPC_POOL_BLOCK_EXHAUSTED} - If {@code true}, threads block when pool is empty, otherwise throws an
 * Exception, default={@code true}</li>
 * <li>{@code GRPC_POOL_ERODING_FACTOR} - Optional shrink rate for idle connections, default={@code -1.0f}</li>
 * <li>{@code GRPC_POOL_MAX_BORROW_WAIT_MILLIS} - Time to wait before failing a borrow attempt,
 * default={@code 10000L}</li>
 * <li>{@code GRPC_POOL_MAX_IDLE_CONNECTIONS} - Maximum idle connections in the pool, default={@code 8}</li>
 * <li>{@code GRPC_POOL_MAX_SIZE} - Maximum total connections allowed, default={@code 8}</li>
 * <li>{@code GRPC_POOL_MIN_IDLE_CONNECTIONS} - Minimum idle connections in the pool, default={@code 0}</li>
 * <li>{@code GRPC_POOL_RETRIEVAL_ORDER} - Whether pool behaves LIFO or FIFO, default={@code "LIFO"}</li>
 * <li>{@code GRPC_POOL_TEST_BEFORE_BORROW} - If {@code true}, validates pooled connections before use with
 * {@link #validateObject(PooledObject)}, default={@code true}</li>
 * </ul>
 */
public class PooledChannelManager extends ChannelManager implements PooledObjectFactory<ManagedChannel> {
    public static final String GRPC_POOL_BLOCK_EXHAUSTED = "GRPC_POOL_BLOCK_EXHAUSTED";
    public static final String GRPC_POOL_ERODING_FACTOR = "GRPC_POOL_ERODING_FACTOR";
    public static final String GRPC_POOL_MAX_BORROW_WAIT_MILLIS = "GRPC_POOL_MAX_BORROW_WAIT_MILLIS";
    public static final String GRPC_POOL_MAX_IDLE_CONNECTIONS = "GRPC_POOL_MAX_IDLE_CONNECTIONS";
    public static final String GRPC_POOL_MAX_SIZE = "GRPC_POOL_MAX_SIZE";
    public static final String GRPC_POOL_MIN_IDLE_CONNECTIONS = "GRPC_POOL_MIN_IDLE_CONNECTIONS";
    public static final String GRPC_POOL_RETRIEVAL_ORDER = "GRPC_POOL_RETRIEVAL_ORDER";
    public static final String GRPC_POOL_TEST_BEFORE_BORROW = "GRPC_POOL_TEST_BEFORE_BORROW";
    
    protected final ObjectPool<ManagedChannel> channelPool;

    /**
     * CConstructs a new gRPC connection manager using the provided host, port, and configuration. Initializes pool
     * settings and gRPC channel properties from the given configuration source.
     *
     * @param host gRPC service hostname or DNS target
     * @param port gRPC service port
     * @param configG configuration provider for channel and pool parameters
     * @param channelValidator method to determine if channel can successfully communicate with its associated server
     * @see ChannelManager
     * @see PooledChannelManager
     */
    public PooledChannelManager(String host, int port, Configurator configG, Predicate<ManagedChannel> channelValidator) {
        super(host, port, configG, channelValidator);
        GenericObjectPoolConfig<ManagedChannel> poolConfig = new GenericObjectPoolConfig<>();

        // Enable thread blocking when borrowing from exhausted pool
        poolConfig.setBlockWhenExhausted(configG.findBooleanEntry(GRPC_POOL_BLOCK_EXHAUSTED,
                BaseObjectPoolConfig.DEFAULT_BLOCK_WHEN_EXHAUSTED));

        // Max duration to wait until block is released from exhausted pool
        poolConfig.setMaxWait(Duration.ofMillis(configG.findLongEntry(GRPC_POOL_MAX_BORROW_WAIT_MILLIS, 10000L)));

        // Min/max number of idle connections in pool
        poolConfig.setMinIdle(configG.findIntEntry(GRPC_POOL_MIN_IDLE_CONNECTIONS, 0));
        poolConfig.setMaxIdle(configG.findIntEntry(GRPC_POOL_MAX_IDLE_CONNECTIONS, 8));

        // Max number of total connections in pool
        poolConfig.setMaxTotal(configG.findIntEntry(GRPC_POOL_MAX_SIZE, 8));

        // Order for pool to borrow connections
        PoolRetrievalOrdering ordering = PoolRetrievalOrdering.getOrder(
                configG.findStringEntry(GRPC_POOL_RETRIEVAL_ORDER), PoolRetrievalOrdering.LIFO);
        poolConfig.setLifo(ordering.equals(PoolRetrievalOrdering.LIFO));

        // Whether to validate channels when borrowing from the pool
        poolConfig.setTestOnBorrow(configG.findBooleanEntry(GRPC_POOL_TEST_BEFORE_BORROW, true));

        // Controls how aggressively idle connections are phased out over time
        // Set to a float between 0.0 and 1.0 to enable erosion (e.g. 0.2 = mild erosion)
        // Set to -1.0 to disable automatic pool shrinking entirely
        float erodingPoolFactor = (float) configG.findDoubleEntry(GRPC_POOL_ERODING_FACTOR, -1.0f);
        if (erodingPoolFactor > 0) {
            channelPool = PoolUtils.erodingPool(new GenericObjectPool<>(this, poolConfig), erodingPoolFactor);
        } else {
            channelPool = new GenericObjectPool<>(this, poolConfig);
        }
    }

    /**
     * Borrows a {@link ManagedChannel} from the connection pool.
     *
     * @return a managed gRPC channel
     * @throws PoolException if the pool is exhausted or borrowing fails
     */
    @Override
    public ManagedChannel acquire() {
        try {
            return channelPool.borrowObject();
        } catch (Exception e) {
            throw new PoolException(String.format("Unable to borrow channel from pool: %s", e.getMessage()), e);
        }
    }

    /**
     * Returns a previously acquired {@link ManagedChannel} to the connection pool. If the return fails, attempts to
     * invalidate the channel to avoid pool corruption.
     *
     * @param channel the channel to return
     */
    @Override
    public void release(ManagedChannel channel) {
        try {
            channelPool.returnObject(channel);
        } catch (Exception e) {
            logger.warn("Unable to cleanly return grpc connection channel to the pool: {}", e.getMessage());
            logger.debug("Stack trace: ", e);
            shutdown(channel);
        }
    }

    /**
     * Invalidates a {@link ManagedChannel}, removing it from the pool and shutting it down. Used when a channel is
     * considered unhealthy or no longer usable.
     *
     * @param channel the channel to invalidate
     */
    @Override
    public void shutdown(ManagedChannel channel) {
        try {
            channelPool.invalidateObject(channel);
        } catch (Exception e) {
            throw new PoolException(String.format(
                    "Unable to invalidate existing grpc connection - check for possible resource leaks: %s", e.getMessage()), e);
        }
    }

    /**
     * Closes the connection pool.
     */
    @Override
    public void close() {
        channelPool.close();
    }

    /**
     * Reinitialize an instance to be returned by the pool. No-op by default.
     *
     * @param pooledObject a {@code PooledObject} wrapping the instance to be activated
     */
    @Override
    public void activateObject(PooledObject<ManagedChannel> pooledObject) {
        /* No op */
    }

    /**
     * Cleans up a pooled {@link ManagedChannel} when it is removed from the pool. Immediately shuts down the channel to
     * release resources.
     *
     * @param pooledObject the pooled gRPC channel to destroy
     */
    @Override
    public void destroyObject(PooledObject<ManagedChannel> pooledObject) {
        pooledObject.getObject().shutdownNow();
    }

    /**
     * Creates an instance that can be served by the pool and wrap it in a PooledObject to be managed by the pool.
     *
     * @return {#link PooledObject} wrapping an instance that can be served by the pool, not null.
     */
    @Override
    public PooledObject<ManagedChannel> makeObject() {
        Supplier<String> messageSupplier = () -> String.format("BasePooledObjectFactory(%s).create() = null", this.getClass().getName());
        return new DefaultPooledObject<>(Objects.requireNonNull(create(), messageSupplier));
    }

    /**
     * Called when a {@link ManagedChannel} is returned to the pool. Since gRPC channels are designed to remain ready for
     * reuse without needing to be reset or cleared, a no-op passivator is fine.
     *
     * @param pooledObject the pooled channel being passivated
     */
    @Override
    public void passivateObject(PooledObject<ManagedChannel> pooledObject) {
        /* No op */
    }

    /**
     * Validates whether the {@link ManagedChannel} is healthy and can be reused. Called by the pool before returning a
     * channel to a caller. Implementations should check connection state or channel health as needed. For example, you
     * might verify that the channel is not shutdown or terminated.
     *
     * @param pooledObject the pooled gRPC channel to validate
     * @return true if the channel is valid and safe to reuse, false otherwise
     */
    @Override
    public boolean validateObject(PooledObject<ManagedChannel> pooledObject) {
        return channelValidator.test(pooledObject.getObject());
    }

    public enum PoolRetrievalOrdering {
        LIFO, FIFO;

        public static PoolRetrievalOrdering getOrder(String input, PoolRetrievalOrdering defaultOrdering) {
            if (input == null) {
                return defaultOrdering;
            }
            return PoolRetrievalOrdering.valueOf(input.toUpperCase(Locale.ROOT));
        }
    }
}
