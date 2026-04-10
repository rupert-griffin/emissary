package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.config.ServiceConfigGuide;
import emissary.grpc.channel.ChannelManager.ChannelValidator;
import emissary.test.core.junit5.UnitTest;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedChannelManagerTest extends UnitTest {
    private static SharedChannelManager newChannelManager(Configurator configT) {
        return newChannelManager(configT, m -> true);
    }

    private static SharedChannelManager newChannelManager(Configurator configT, ChannelValidator validator) {
        return new SharedChannelManager("localhost", 1234, configT, validator);
    }

    @Test
    void testChannelValidationFails() {
        try (SharedChannelManager manager = newChannelManager(new ServiceConfigGuide(), m -> false)) {
            StatusRuntimeException e = assertThrows(StatusRuntimeException.class, manager::acquire);
            assertEquals("UNAVAILABLE: It's likely service crashed", e.getMessage());
        }
    }

    @Test
    void testChannelShutdown() {
        try (SharedChannelManager manager = newChannelManager(new ServiceConfigGuide())) {
            ManagedChannel channel = manager.acquire();

            assertFalse(channel.isShutdown());

            manager.shutdown(channel);

            assertTrue(channel.isShutdown());
        }
    }

    @Test
    void testChannelReleaseNoOp() {
        try (SharedChannelManager manager = newChannelManager(new ServiceConfigGuide())) {
            ManagedChannel channel = manager.acquire();

            assertFalse(channel.isShutdown());

            manager.release(channel);

            assertFalse(channel.isShutdown());
        }
    }

    @Test
    void testCloseManager() {
        SharedChannelManager manager = newChannelManager(new ServiceConfigGuide());
        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.close();

        assertTrue(channel.isShutdown());
        assertNotSame(channel, manager.acquire());
    }
}
