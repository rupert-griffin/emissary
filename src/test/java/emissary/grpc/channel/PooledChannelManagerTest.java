package emissary.grpc.channel;

import emissary.config.ConfigEntry;
import emissary.config.Configurator;
import emissary.config.ServiceConfigGuide;
import emissary.grpc.channel.ChannelManager.ChannelValidator;
import emissary.grpc.channel.LegacyPooledChannelManager.PoolRetrievalOrdering;
import emissary.test.core.junit5.UnitTest;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PooledChannelManagerTest extends UnitTest {
    private static LegacyPooledChannelManager newChannelManager(Configurator configT) {
        return newChannelManager(configT, m -> true);
    }

    private static LegacyPooledChannelManager newChannelManager(Configurator configT, ChannelValidator validator) {
        return new LegacyPooledChannelManager("localhost", 1234, configT, validator);
    }

    private static Configurator buildConfigs(ConfigEntry... configEntries) {
        Configurator configT = new ServiceConfigGuide();
        Arrays.stream(configEntries).forEach(c -> configT.addEntry(c.getKey(), c.getValue()));
        configT.addEntry(LegacyPooledChannelManager.MIN_IDLE_CONNECTIONS, "1");
        configT.addEntry(LegacyPooledChannelManager.MAX_IDLE_CONNECTIONS, "2");
        configT.addEntry(LegacyPooledChannelManager.MAX_SIZE, "2");
        return configT;
    }

    @Test
    void testBadPoolRetrievalOrderConfig() {
        Runnable invocation = () -> newChannelManager(buildConfigs(
                new ConfigEntry(LegacyPooledChannelManager.RETRIEVAL_ORDER, "ZIFO"))).close();
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, invocation::run);
        assertEquals("No enum constant " +
                "emissary.grpc.channel.PooledChannelManager.PoolRetrievalOrdering.ZIFO", e.getMessage());
    }

    @Test
    void testLifoPoolRetrievalOrderConfig() {
        try (PooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(PooledChannelManager.RETRIEVAL_ORDER, PoolRetrievalOrdering.LIFO.name())))) {

            ManagedChannel inChannelFirst = manager.acquire();
            ManagedChannel inChannelLast = manager.acquire();

            assertNotSame(inChannelFirst, inChannelLast);

            manager.release(inChannelFirst);
            manager.release(inChannelLast);

            ManagedChannel outChannel = manager.acquire();

            assertNotSame(inChannelFirst, outChannel);
            assertSame(inChannelLast, outChannel);
        }
    }

    @Test
    void testFifoPoolRetrievalOrderConfig() {
        try (LegacyPooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(LegacyPooledChannelManager.RETRIEVAL_ORDER, PoolRetrievalOrdering.FIFO.name())))) {

            ManagedChannel inChannelFirst = manager.acquire();
            ManagedChannel inChannelLast = manager.acquire();

            assertNotSame(inChannelFirst, inChannelLast);

            manager.release(inChannelFirst);
            manager.release(inChannelLast);

            ManagedChannel outChannel = manager.acquire();

            assertSame(inChannelFirst, outChannel);
            assertNotSame(inChannelLast, outChannel);
        }
    }

    @Test
    void testValidationFails() {
        try (LegacyPooledChannelManager manager = newChannelManager(buildConfigs(), m -> false)) {
            StatusRuntimeException e = assertThrows(StatusRuntimeException.class, manager::acquire);
            assertEquals("UNAVAILABLE: It's likely service crashed", e.getMessage());
        }
    }

    @Test
    void testReleaseUnavailableChannel() {
        try (LegacyPooledChannelManager manager = newChannelManager(buildConfigs())) {
            ManagedChannel channel = manager.acquire();

            assertFalse(channel.isShutdown());

            manager.release(channel);

            assertFalse(channel.isShutdown());

            manager.release(channel);

            assertTrue(channel.isShutdown());
        }
    }

    @Test
    void testShutdownUnavailableChannel() {
        try (LegacyPooledChannelManager manager = newChannelManager(buildConfigs())) {
            ManagedChannel channel = manager.acquire();

            assertFalse(channel.isShutdown());

            manager.shutdown(channel);

            assertTrue(channel.isShutdown());

            IllegalStateException e = assertThrows(IllegalStateException.class, () -> manager.shutdown(channel));
            assertEquals("Invalidated object not currently part of this pool", e.getMessage());
        }
    }

    @SuppressWarnings("java:S2925") // sleep warning
    @Test
    void testPoolErosion() throws InterruptedException {
        try (LegacyPooledChannelManager manager = newChannelManager(buildConfigs(
                new ConfigEntry(LegacyPooledChannelManager.ERODING_FACTOR, "0.0001")))) {
            ManagedChannel alive = manager.acquire();
            ManagedChannel dead = manager.acquire();

            manager.release(alive);
            Thread.sleep(Duration.ofMillis(250).toMillis());
            manager.release(dead);

            assertFalse(alive.isShutdown());
            assertTrue(dead.isShutdown());
        }
    }

    @Test
    void testPoolClose() {
        LegacyPooledChannelManager manager = newChannelManager(buildConfigs());
        manager.close();

        IllegalStateException e = assertThrows(IllegalStateException.class, manager::acquire);
        assertEquals("Pool not open", e.getMessage());
    }
}
