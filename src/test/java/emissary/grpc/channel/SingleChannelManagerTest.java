package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.config.ServiceConfigGuide;
import emissary.grpc.exceptions.ServiceNotLiveException;
import emissary.test.core.junit5.UnitTest;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SingleChannelManagerTest extends UnitTest {
    private static SingleChannelManager newChannelManager(Configurator configT) {
        return newChannelManager(configT, m -> true);
    }

    private static SingleChannelManager newChannelManager(Configurator configT, Predicate<ManagedChannel> validator) {
        return new SingleChannelManager("localhost", 1234, configT, validator);
    }

    @Test
    void testChannelValidationFails() {
        Runnable invocation = () -> newChannelManager(new ServiceConfigGuide(), m -> false);

        ServiceNotLiveException e = assertThrows(ServiceNotLiveException.class, invocation::run);
        assertEquals("Unable to validate gRPC connection", e.getMessage());
    }

    @Test
    void testChannelShutdown() {
        SingleChannelManager manager = newChannelManager(new ServiceConfigGuide());

        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.shutdown(channel);

        assertTrue(channel.isShutdown());
    }

    @Test
    void testChannelReleaseNoOp() {
        SingleChannelManager manager = newChannelManager(new ServiceConfigGuide());

        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.release(channel);

        assertFalse(channel.isShutdown());
    }

    @Test
    void testCloseManager() {
        SingleChannelManager manager = newChannelManager(new ServiceConfigGuide());
        ManagedChannel channel = manager.acquire();

        assertFalse(channel.isShutdown());

        manager.close();

        assertTrue(channel.isShutdown());
        assertNull(manager.acquire());
    }
}
