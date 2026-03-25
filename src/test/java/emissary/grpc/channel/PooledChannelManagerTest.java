package emissary.grpc.channel;

import emissary.config.ConfigEntry;
import emissary.config.Configurator;
import emissary.config.ServiceConfigGuide;
import emissary.grpc.channel.PooledChannelManager.PoolRetrievalOrdering;
import emissary.grpc.exceptions.PoolException;
import emissary.test.core.junit5.UnitTest;

import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PooledChannelManagerTest extends UnitTest {
    private static PooledChannelManager newChannelManager(Configurator configT) {
        return newChannelManager(configT, m -> true);
    }

    private static PooledChannelManager newChannelManager(Configurator configT, Predicate<ManagedChannel> validator) {
        return new PooledChannelManager("localhost", 1234, configT, validator);
    }

    private static Configurator buildConfigs(ConfigEntry... configEntries) {
        Configurator configT = new ServiceConfigGuide();
        Arrays.stream(configEntries).forEach(c -> configT.addEntry(c.getKey(), c.getValue()));
        configT.addEntry(PooledChannelManager.GRPC_POOL_MIN_IDLE_CONNECTIONS, "1");
        configT.addEntry(PooledChannelManager.GRPC_POOL_MAX_IDLE_CONNECTIONS, "2");
        configT.addEntry(PooledChannelManager.GRPC_POOL_MAX_SIZE, "2");
        return configT;
    }

    @Test
    void testBadPoolRetrievalOrderConfig() {
        Configurator configT = buildConfigs(new ConfigEntry(PooledChannelManager.GRPC_POOL_RETRIEVAL_ORDER, "ZIFO"));

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> newChannelManager(configT));
        assertEquals("No enum constant " +
                "emissary.grpc.channel.PooledChannelManager.PoolRetrievalOrdering.ZIFO", e.getMessage());
    }

    @Test
    void testLifoPoolRetrievalOrderConfig() {
        PooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(PooledChannelManager.GRPC_POOL_RETRIEVAL_ORDER, PoolRetrievalOrdering.LIFO.name())));

        ManagedChannel inChannelFirst = manager.acquire();
        ManagedChannel inChannelLast = manager.acquire();

        assertNotSame(inChannelFirst, inChannelLast);

        manager.release(inChannelFirst);
        manager.release(inChannelLast);

        ManagedChannel outChannel = manager.acquire();

        assertNotSame(inChannelFirst, outChannel);
        assertSame(inChannelLast, outChannel);
    }

    @Test
    void testFifoPoolRetrievalOrderConfig() {
        PooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(PooledChannelManager.GRPC_POOL_RETRIEVAL_ORDER, PoolRetrievalOrdering.FIFO.name())));

        ManagedChannel inChannelFirst = manager.acquire();
        ManagedChannel inChannelLast = manager.acquire();

        assertNotSame(inChannelFirst, inChannelLast);

        manager.release(inChannelFirst);
        manager.release(inChannelLast);

        ManagedChannel outChannel = manager.acquire();

        assertSame(inChannelFirst, outChannel);
        assertNotSame(inChannelLast, outChannel);
    }

    @Test
    void testValidationFails() {
        PooledChannelManager manager = newChannelManager(buildConfigs(), m -> false);

        PoolException e = assertThrows(PoolException.class, manager::acquire);
        assertEquals("Unable to borrow channel from pool: Unable to validate object", e.getMessage());
    }

    @Test
    void testAcquireWithoutValidation() {
        PooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(PooledChannelManager.GRPC_POOL_TEST_BEFORE_BORROW, Boolean.FALSE.toString())), m -> false);

        assertDoesNotThrow(manager::acquire);
    }

    @Test
    void testReleaseUnavailableChannel() {
        PooledChannelManager manager = newChannelManager(buildConfigs());

        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.release(channel);

        assertFalse(channel.isShutdown());

        manager.release(channel);

        assertTrue(channel.isShutdown());
    }

    @Test
    void testShutdownUnavailableChannel() {
        PooledChannelManager manager = newChannelManager(buildConfigs());

        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.shutdown(channel);

        assertTrue(channel.isShutdown());

        PoolException e = assertThrows(PoolException.class, () -> manager.shutdown(channel));
        assertEquals("Unable to invalidate existing grpc connection - " +
                        "check for possible resource leaks: " +
                        "Invalidated object not currently part of this pool", e.getMessage());
    }

    @SuppressWarnings("java:S2925") // sleep warning
    @Test
    void testPoolErosion() throws InterruptedException {
        PooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(PooledChannelManager.GRPC_POOL_ERODING_FACTOR, "0.0001")));

        ManagedChannel alive = manager.acquire();
        ManagedChannel dead = manager.acquire();

        manager.release(alive);
        Thread.sleep(Duration.ofMillis(250).toMillis());
        manager.release(dead);

        assertFalse(alive.isShutdown());
        assertTrue(dead.isShutdown());
    }

    @Test
    void testPoolClose() {
        PooledChannelManager manager = newChannelManager(buildConfigs());
        manager.close();

        PoolException e = assertThrows(PoolException.class, manager::acquire);
        assertEquals("Unable to borrow channel from pool: Pool not open", e.getMessage());
    }
}
