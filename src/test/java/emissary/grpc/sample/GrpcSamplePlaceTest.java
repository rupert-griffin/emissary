package emissary.grpc.sample;

import emissary.config.ConfigEntry;
import emissary.config.ServiceConfigGuide;
import emissary.core.BaseDataObject;
import emissary.core.IBaseDataObject;
import emissary.core.constants.Configurations;
import emissary.grpc.GrpcRoutingPlace;
import emissary.grpc.retry.RetryHandler;
import emissary.grpc.sample.v1.SampleResponse;
import emissary.test.core.junit5.UnitTest;
import emissary.test.util.ConfiguredPlaceFactory;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpcSamplePlaceTest extends UnitTest {
    private final ConfiguredPlaceFactory<GrpcSamplePlace> placeFactory = new ConfiguredPlaceFactory<>(
            GrpcSamplePlace.class, new ServiceConfigGuide(),
            new ConfigEntry(Configurations.PLACE_NAME, "GrpcSamplePlace"),
            new ConfigEntry(Configurations.SERVICE_NAME, "GRPC_SAMPLE_SERVICE"),
            new ConfigEntry(Configurations.SERVICE_TYPE, "TRANSFORM"),
            new ConfigEntry(Configurations.SERVICE_COST, "50"),
            new ConfigEntry(Configurations.SERVICE_QUALITY, "50"),
            new ConfigEntry(Configurations.SERVICE_PROXY, "*"));

    public static final String ENDPOINT_1 = "EP1";
    public static final String ENDPOINT_2 = "EP2";
    public static final String LOCALHOST = "localhost";
    private static final byte[] INPUT_DATA = "Data123!".getBytes();

    private IBaseDataObject o;

    private GrpcSamplePlace buildPlaceWithEndpoints(GrpcSampleServer serverOne, GrpcSampleServer serverTwo, ConfigEntry... configEntries) {
        ConfigEntry[] configs = Stream.concat(
                Arrays.stream(configEntries),
                Arrays.stream(new ConfigEntry[] {
                        new ConfigEntry(GrpcSamplePlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                        new ConfigEntry(GrpcSamplePlace.GRPC_PORT + ENDPOINT_1, serverOne.getPort()),
                        new ConfigEntry(GrpcSamplePlace.GRPC_HOST + ENDPOINT_2, LOCALHOST),
                        new ConfigEntry(GrpcSamplePlace.GRPC_PORT + ENDPOINT_2, serverTwo.getPort())
                })).toArray(ConfigEntry[]::new);
        return placeFactory.buildPlace(configs);
    }

    @BeforeEach
    void initializeDataObject() {
        o = new BaseDataObject();
        o.setData(INPUT_DATA);
    }

    @Nested
    class ConfigurationTests {
        @Test
        void testNoConfiguredEndpoints() {
            NullPointerException e = placeFactory.getBuildPlaceException(NullPointerException.class);
            assertEquals("Missing required arguments: GRPC_HOST_${Target-ID} and GRPC_PORT_${Target-ID}", e.getMessage());
        }

        @ParameterizedTest
        @CsvSource(value = {
                GrpcRoutingPlace.GRPC_HOST + "," + LOCALHOST,
                GrpcRoutingPlace.GRPC_PORT + "," + "1234"})
        void testIncompleteEndpointConfigurations(String cfgKey, String cfgVal) {
            IllegalArgumentException e = placeFactory.getBuildPlaceException(IllegalArgumentException.class,
                    new ConfigEntry(cfgKey, cfgVal));
            assertEquals("gRPC hostname target-IDs do not match gRPC port number target-IDs", e.getMessage());
        }

        @Test
        void testMismatchedHostPortIds() {
            IllegalArgumentException e = placeFactory.getBuildPlaceException(IllegalArgumentException.class,
                    new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_2, "2"));
            assertEquals("gRPC hostname target-IDs do not match gRPC port number target-IDs", e.getMessage());
        }

        @Test
        void testSingleEndpointConfigurationBuilds() {
            Runnable invocation = () -> placeFactory.buildPlace(
                    new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_1, "1")).close();
            assertDoesNotThrow(invocation::run);
        }

        @Test
        void testMultipleEndpointConfigurationBuilds() {
            Runnable invocation = () -> placeFactory.buildPlace(
                        new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                        new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_2, LOCALHOST),
                        new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_1, "1"),
                        new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_2, "2")).close();
            assertDoesNotThrow(invocation::run);
        }
    }

    @Nested
    class EndpointRoutingTests {
        private final byte[] endpointOneMessage = "1".getBytes();
        private final byte[] endpointTwoMessage = "2".getBytes();

        @Test
        void testEndpointOneServiceRouting() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointTwoMessage));
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Objects.requireNonNull(place).processEndpoint(o, ENDPOINT_1);
                assertArrayEquals(endpointOneMessage, o.getAlternateView(ENDPOINT_1));
                assertFalse(o.getAlternateViewNames().contains(ENDPOINT_2));
            }
        }

        @Test
        void testEndpointTwoServiceRouting() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointTwoMessage));
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Objects.requireNonNull(place).processEndpoint(o, ENDPOINT_2);
                assertArrayEquals(endpointTwoMessage, o.getAlternateView(ENDPOINT_2));
                assertFalse(o.getAlternateViewNames().contains(ENDPOINT_1));
            }
        }

        @Test
        void testInvalidRouting() {
            String invalidEndpoint = "invalid";
            try (GrpcSampleServer serverOne = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = GrpcSampleServer.of(req -> ByteString.copyFrom(endpointTwoMessage));
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Runnable invocation = () -> Objects.requireNonNull(place).processEndpoint(o, invalidEndpoint);
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class, invocation::run);
                assertEquals(String.format("Target-ID %s was never configured", invalidEndpoint), e.getMessage());
            }
        }
    }

    @Nested
    abstract class RetryDisabledTests {
        protected static final int RETRY_MAX = 1;
        protected final ConfigEntry retryMaxConfig =
                new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_MAX));

        protected abstract void process(GrpcSamplePlace place, IBaseDataObject data);

        @Test
        void testGrpcSuccess() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.defaultBehavior();
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
            }
        }

        @ParameterizedTest
        @EnumSource(value = Status.Code.class, names = {"OK"}, mode = EnumSource.Mode.EXCLUDE)
        void testGrpcFailureAfterExceptionCode(Status.Code code) {
            Status status = Status.fromCode(code);
            try (GrpcSampleServer serverOne = GrpcSampleServer.alwaysThrow(new StatusRuntimeException(status));
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
            }
        }

        @Test
        void testGrpcFailureAfterRuntimeException() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.alwaysThrow(new IllegalStateException());
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
                assertEquals(Status.Code.UNKNOWN.name(), e.getMessage());
                assertTrue(o.getAlternateViewNames().isEmpty());
            }
        }
    }

    @Nested
    abstract class RetryEnabledTests {
        protected static final int RETRY_MAX = 5;
        protected final ConfigEntry retryMaxConfig =
                new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_MAX));

        protected AtomicInteger retryCounter;
        protected AtomicInteger baselineCounter;

        protected abstract void process(GrpcSamplePlace place, IBaseDataObject data);

        @BeforeEach
        void setCounters() {
            retryCounter = new AtomicInteger(0);
            baselineCounter = new AtomicInteger(0);
        }

        @Test
        void testGrpcSuccessFirstTry() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.throwAfter(1, retryCounter, new IllegalStateException());
                    GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                assertEquals(1, retryCounter.get());
                assertTrue(1 >= baselineCounter.get());
            }
        }


        @ParameterizedTest
        @EnumSource(
                value = Status.Code.class,
                names = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"},
                mode = EnumSource.Mode.INCLUDE)
        void testGrpcSuccessAfterRecoverableCodes(Status.Code code) {
            Status status = Status.fromCode(code);

            try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(RETRY_MAX, retryCounter, new StatusRuntimeException(status));
                    GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                assertEquals(RETRY_MAX, retryCounter.get());
                assertTrue(1 >= baselineCounter.get());
            }
        }

        @ParameterizedTest
        @EnumSource(
                value = Status.Code.class,
                names = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"},
                mode = EnumSource.Mode.INCLUDE)
        void testGrpcFailureAfterMaxRecoverableCodes(Status.Code code) {
            Status status = Status.fromCode(code);
            int retryMax = RETRY_MAX + 1;

            try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(retryMax, retryCounter, new StatusRuntimeException(status));
                    GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(RETRY_MAX, retryCounter.get());
                assertTrue(1 >= baselineCounter.get());
            }
        }

        @ParameterizedTest
        @EnumSource(
                value = Status.Code.class,
                names = {"OK", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"},
                mode = EnumSource.Mode.EXCLUDE)
        void testGrpcFailureAfterNonRecoverableCode(Status.Code code) {
            Status status = Status.fromCode(code);

            try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(RETRY_MAX, retryCounter, new StatusRuntimeException(status));
                    GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(1, retryCounter.get());
                assertTrue(1 >= baselineCounter.get());
            }
        }

        @Test
        void testGrpcFailureAfterRuntimeExceptions() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(RETRY_MAX, retryCounter, new IllegalStateException());
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
                assertEquals(Status.Code.UNKNOWN.name(), e.getMessage());
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(1, retryCounter.get());
                assertTrue(1 >= baselineCounter.get());
            }
        }
    }

    @Nested
    class SequentialProcessingTests {
        @Test
        void testConnectionIsNotValidated() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.of(false);
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Runnable invocation = () -> Objects.requireNonNull(place).processEndpointsSequentially(o);
                StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, invocation::run);
                assertEquals("UNAVAILABLE: It's likely service crashed", exception.getMessage());
            }
        }

        @Test
        void testBlockedResponsesAreProcessedSequentially() throws InterruptedException {
            CountDownLatch startedLatchOne = new CountDownLatch(1);
            CountDownLatch releaseLatchOne = new CountDownLatch(1);
            CountDownLatch startedLatchTwo = new CountDownLatch(1);
            CountDownLatch releaseLatchTwo = new CountDownLatch(1);

            try (GrpcSampleServer serverOne = GrpcSampleServer.blockUntilReleased(startedLatchOne, releaseLatchOne);
                    GrpcSampleServer serverTwo = GrpcSampleServer.blockUntilReleased(startedLatchTwo, releaseLatchTwo);
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Map<String, byte[]> views = o.getAlternateViews();
                AtomicReference<Throwable> errorRef = new AtomicReference<>();

                Thread clientThread = new Thread(() -> {
                    try {
                        Objects.requireNonNull(place).processEndpointsSequentially(o);
                    } catch (Throwable t) {
                        errorRef.set(t);
                    }
                });

                clientThread.start();

                assertTrue(startedLatchOne.await(1, TimeUnit.SECONDS), "First server should have received request");
                assertEquals(1L, startedLatchTwo.getCount(), "Second server should be waiting to receive request");
                assertTrue(views.isEmpty());
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatchOne.countDown();

                assertTrue(startedLatchTwo.await(1, TimeUnit.SECONDS), "Second server should have received request");
                assertEquals(1, views.size());
                assertArrayEquals(INPUT_DATA, views.get(ENDPOINT_1));
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatchTwo.countDown();

                clientThread.join(1000); // Wait for thread to die
                assertFalse(clientThread.isAlive());

                assertEquals(2, views.size());
                assertArrayEquals(INPUT_DATA, views.get(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, views.get(ENDPOINT_2));
                assertNull(errorRef.get());
            }
        }

        @Nested
        class SequentialRetryDisabledTests extends RetryDisabledTests {
            @Override
            protected void process(GrpcSamplePlace place, IBaseDataObject data) {
                Objects.requireNonNull(place).processEndpointsSequentially(data);
            }
        }

        @Nested
        class SequentialRetryEnabledTests extends RetryEnabledTests {
            @Override
            protected void process(GrpcSamplePlace place, IBaseDataObject data) {
                Objects.requireNonNull(place).processEndpointsSequentially(data);
            }
        }
    }

    @Nested
    class ParallelProcessingTests {
        @Test
        void testConnectionIsNotValidated() {
            try (GrpcSampleServer serverOne = GrpcSampleServer.of(false);
                    GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Runnable invocation = () -> Objects.requireNonNull(place).processEndpointsInParallel(o, null);
                StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, invocation::run);
                assertEquals("UNAVAILABLE: It's likely service crashed", exception.getMessage());
            }
        }

        @Test
        void testFutureResponsesAreProcessedInParallel() throws InterruptedException {
            CountDownLatch startedLatch = new CountDownLatch(2);
            CountDownLatch releaseLatch = new CountDownLatch(1);

            try (GrpcSampleServer serverOne = GrpcSampleServer.blockUntilReleased(startedLatch, releaseLatch);
                    GrpcSampleServer serverTwo = GrpcSampleServer.blockUntilReleased(startedLatch, releaseLatch);
                    GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo)) {

                Map<String, byte[]> views = o.getAlternateViews();
                AtomicReference<Throwable> errorRef = new AtomicReference<>();

                Thread clientThread = new Thread(() -> {
                    try {
                        Objects.requireNonNull(place).processEndpointsInParallel(o, null);
                    } catch (Throwable t) {
                        errorRef.set(t);
                    }
                });

                clientThread.start();

                assertTrue(startedLatch.await(2, TimeUnit.SECONDS), "Not all servers received requests");
                assertTrue(views.isEmpty());
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatch.countDown();

                clientThread.join(1000); // Wait for thread to die
                assertFalse(clientThread.isAlive());

                assertEquals(2, views.size());
                assertArrayEquals(INPUT_DATA, views.get(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, views.get(ENDPOINT_2));
                assertNull(errorRef.get());
            }
        }

        @Nested
        class ParallelRetryDisabledTests extends RetryDisabledTests {
            @Override
            protected void process(GrpcSamplePlace place, IBaseDataObject data) {
                Objects.requireNonNull(place).processEndpointsInParallel(data, null);
            }

            @ParameterizedTest
            @EnumSource(value = Status.Code.class, names = {"OK"}, mode = EnumSource.Mode.EXCLUDE)
            void testGrpcFailureAfterExceptionCodeWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);
                try (GrpcSampleServer serverOne = GrpcSampleServer.alwaysThrow(new StatusRuntimeException(status));
                        GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                        GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                    Objects.requireNonNull(place).processEndpointsInParallel(o, t -> SampleResponse.getDefaultInstance());

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                }
            }

            @Test
            void testGrpcFailureAfterRuntimeExceptionWithDefaultResponse() {
                try (GrpcSampleServer serverOne = GrpcSampleServer.alwaysThrow(new IllegalStateException());
                        GrpcSampleServer serverTwo = GrpcSampleServer.defaultBehavior();
                        GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                    Objects.requireNonNull(place).processEndpointsInParallel(o, t -> SampleResponse.getDefaultInstance());

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                }
            }
        }

        @Nested
        class ParallelRetryEnabledTests extends RetryEnabledTests {
            @Override
            protected void process(GrpcSamplePlace place, IBaseDataObject data) {
                Objects.requireNonNull(place).processEndpointsInParallel(data, null);
            }

            @ParameterizedTest
            @EnumSource(
                    value = Status.Code.class,
                    names = {"UNAVAILABLE", "DEADLINE_EXCEEDED", "RESOURCE_EXHAUSTED"},
                    mode = EnumSource.Mode.INCLUDE)
            void testGrpcFailureAfterMaxRecoverableCodesWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);
                int attemptMax = RETRY_MAX + 1;

                try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(attemptMax, retryCounter, new StatusRuntimeException(status));
                        GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                        GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                    Objects.requireNonNull(place).processEndpointsInParallel(o, t -> SampleResponse.getDefaultInstance());

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(RETRY_MAX, retryCounter.get());
                    assertTrue(1 >= baselineCounter.get());
                }
            }

            @ParameterizedTest
            @EnumSource(
                    value = Status.Code.class,
                    names = {"OK", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"},
                    mode = EnumSource.Mode.EXCLUDE)
            void testGrpcFailureAfterNonRecoverableCodeWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);

                try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(RETRY_MAX, retryCounter, new StatusRuntimeException(status));
                        GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                        GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                    Objects.requireNonNull(place).processEndpointsInParallel(o, t -> SampleResponse.getDefaultInstance());

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(1, retryCounter.get());
                    assertTrue(1 >= baselineCounter.get());
                }
            }

            @Test
            void testGrpcFailureAfterRuntimeExceptionWithDefaultResponse() {
                try (GrpcSampleServer serverOne = GrpcSampleServer.throwUntil(RETRY_MAX, retryCounter, new IllegalStateException());
                        GrpcSampleServer serverTwo = GrpcSampleServer.throwAfter(1, baselineCounter, new IllegalStateException());
                        GrpcSamplePlace place = buildPlaceWithEndpoints(serverOne, serverTwo, retryMaxConfig)) {

                    Objects.requireNonNull(place).processEndpointsInParallel(o, t -> SampleResponse.getDefaultInstance());

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(1, retryCounter.get());
                    assertTrue(1 >= baselineCounter.get());
                }
            }
        }
    }
}
