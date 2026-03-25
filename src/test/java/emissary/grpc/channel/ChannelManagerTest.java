package emissary.grpc.channel;

import emissary.config.Configurator;
import emissary.config.ServiceConfigGuide;
import emissary.test.core.junit5.UnitTest;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChannelManagerTest extends UnitTest {
    @Test
    void testSuccessfulDefaultInitialization() {
        NullChannelManager manager = new NullChannelManager(new ServiceConfigGuide());

        assertEquals("localhost", manager.host);
        assertEquals(1234, manager.port);
        assertEquals("localhost:1234", manager.target);
        assertEquals(60000L, manager.keepAliveMillis);
        assertEquals(30000L, manager.keepAliveTimeoutMillis);
        assertFalse(manager.keepAliveWithoutCalls);
        assertEquals("round_robin", manager.loadBalancingPolicy);
        assertEquals(4194304, manager.maxInboundMessageByteSize);
        assertEquals(8192, manager.maxInboundMetadataByteSize);
    }

    @Test
    void testBadLoadBalancingConfig() {
        Configurator configT = new ServiceConfigGuide();
        configT.addEntry(ChannelManager.GRPC_LOAD_BALANCING_POLICY, "bad_scheduler");

        Runnable invocation = () -> new NullChannelManager(configT);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, invocation::run);
        assertEquals("No enum constant emissary.grpc.channel.ChannelManager.LoadBalancingPolicy.BAD_SCHEDULER", e.getMessage());
    }

    @Test
    void testRoundRobinLoadBalancingConfig() {
        Configurator configT = new ServiceConfigGuide();
        configT.addEntry(PooledChannelManager.GRPC_LOAD_BALANCING_POLICY, ChannelManager.LoadBalancingPolicy.ROUND_ROBIN.name());

        NullChannelManager manager = new NullChannelManager(configT);
        assertEquals("round_robin", manager.loadBalancingPolicy);
    }

    @Test
    void testPickFirstLoadBalancingConfig() {
        Configurator configT =  new ServiceConfigGuide();
        configT.addEntry(PooledChannelManager.GRPC_LOAD_BALANCING_POLICY, ChannelManager.LoadBalancingPolicy.PICK_FIRST.name());

        NullChannelManager manager = new NullChannelManager(configT);
        assertEquals("pick_first", manager.loadBalancingPolicy);
    }

    private static class NullChannelManager extends ChannelManager {
        public NullChannelManager(Configurator configG) {
            super("localhost", 1234, configG, m -> true);
        }

        @Override
        public ManagedChannel acquire() {
            return null;
        }

        @Override
        public void release(ManagedChannel channel) {
            /* No-op */
        }

        @Override
        public void shutdown(ManagedChannel channel) {
            /* No-op */
        }

        @Override
        public void close() {
            /* No-op */
        }
    }
}
