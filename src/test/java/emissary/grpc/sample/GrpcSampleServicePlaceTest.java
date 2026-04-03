package emissary.grpc.sample;

import emissary.config.ConfigEntry;
import emissary.config.ServiceConfigGuide;
import emissary.core.BaseDataObject;
import emissary.core.IBaseDataObject;
import emissary.core.constants.Configurations;
import emissary.grpc.GrpcRoutingPlace;
import emissary.grpc.pool.PoolException;
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

class GrpcSampleServicePlaceTest extends UnitTest {
    private static final String SERVICE_KEY = "*.GRPC_SAMPLE_SERVICE.TRANSFORM." +
            "@{emissary.node.scheme}://@{emissary.node.name}:@{emissary.node.port}/GrpcSampleServicePlace$5050";
    private static final String ENDPOINT_1 = "EP1";
    private static final String ENDPOINT_2 = "EP2";
    private static final String LOCALHOST = "localhost";
    private static final byte[] INPUT_DATA = "Data123!".getBytes();

    private final ConfiguredPlaceFactory<GrpcSampleServicePlace> placeFactory = new ConfiguredPlaceFactory<>(
            GrpcSampleServicePlace.class, new ServiceConfigGuide(),
            new ConfigEntry(Configurations.SERVICE_KEY, SERVICE_KEY));

    private IBaseDataObject o;

    private GrpcSampleServicePlace buildPlaceWithEndpoints(
            GrpcSampleServer serverOne, GrpcSampleServer serverTwo, ConfigEntry... configEntries) {
        return placeFactory.buildPlace(Stream.concat(Arrays.stream(configEntries), Arrays.stream(new ConfigEntry[] {
                new ConfigEntry(GrpcSampleServicePlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                new ConfigEntry(GrpcSampleServicePlace.GRPC_PORT + ENDPOINT_1, serverOne.getPort()),
                new ConfigEntry(GrpcSampleServicePlace.GRPC_HOST + ENDPOINT_2, LOCALHOST),
                new ConfigEntry(GrpcSampleServicePlace.GRPC_PORT + ENDPOINT_2, serverTwo.getPort())
        })).toArray(ConfigEntry[]::new));
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
            assertDoesNotThrow(() -> placeFactory.buildPlace(
                    new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_1, "1")));
        }

        @Test
        void testMultipleEndpointConfigurationBuilds() {
            assertDoesNotThrow(() -> placeFactory.buildPlace(
                    new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_1, LOCALHOST),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_HOST + ENDPOINT_2, LOCALHOST),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_1, "1"),
                    new ConfigEntry(GrpcRoutingPlace.GRPC_PORT + ENDPOINT_2, "2")));
        }
    }

    @Nested
    class EndpointRoutingTests {
        private final byte[] endpointOneMessage = "1".getBytes();
        private final byte[] endpointTwoMessage = "2".getBytes();

        @Test
        void testEndpointOneServiceRouting() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = new GrpcSampleServer(req -> ByteString.copyFrom(endpointTwoMessage))) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                place.processEndpointsSequentially(ENDPOINT_1, o);
                assertArrayEquals(endpointOneMessage, o.getAlternateView(ENDPOINT_1));
                assertFalse(o.getAlternateViewNames().contains(ENDPOINT_2));
            }
        }

        @Test
        void testEndpointTwoServiceRouting() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = new GrpcSampleServer(req -> ByteString.copyFrom(endpointTwoMessage))) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                place.processEndpointsSequentially(ENDPOINT_2, o);
                assertArrayEquals(endpointTwoMessage, o.getAlternateView(ENDPOINT_2));
                assertFalse(o.getAlternateViewNames().contains(ENDPOINT_1));
            }
        }

        @Test
        void testInvalidRouting() {
            String invalidEndpoint = "invalid";
            try (GrpcSampleServer serverOne = new GrpcSampleServer(req -> ByteString.copyFrom(endpointOneMessage));
                    GrpcSampleServer serverTwo = new GrpcSampleServer(req -> ByteString.copyFrom(endpointTwoMessage))) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                Runnable invocation = () -> place.processEndpointsSequentially(invalidEndpoint, o);
                IllegalArgumentException e = assertThrows(IllegalArgumentException.class, invocation::run);
                assertEquals(String.format("Target-ID %s was never configured", invalidEndpoint), e.getMessage());
            }
        }
    }

    abstract class RetryDisabledTests {
        protected static final int RETRY_ATTEMPTS = 1;

        protected abstract void process(GrpcSampleServicePlace place, IBaseDataObject data);

        @Test
        void testGrpcSuccess() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer();
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
            }
        }

        @ParameterizedTest
        @EnumSource(value = Status.Code.class, names = {"OK"}, mode = EnumSource.Mode.EXCLUDE)
        void testGrpcFailureAfterExceptionCode(Status.Code code) {
            Status status = Status.fromCode(code);
            try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status));
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
            }
        }

        @Test
        void testGrpcFailureAfterRuntimeException() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(new IllegalStateException("failed"));
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
                assertEquals(Status.Code.UNKNOWN.name(), e.getMessage());
                assertTrue(o.getAlternateViewNames().isEmpty());
            }
        }
    }

    abstract class RetryEnabledTests {
        protected static final int RETRY_ATTEMPTS = 5;
        protected static final int BASELINE_ATTEMPTS = 1;

        protected AtomicInteger retryAttemptNumber;
        protected AtomicInteger baselineAttemptNumber;

        public abstract void process(GrpcSampleServicePlace place, IBaseDataObject data);

        @BeforeEach
        void setAttemptNumbers() {
            retryAttemptNumber = new AtomicInteger(0);
            baselineAttemptNumber = new AtomicInteger(0);
        }

        @Test
        void testGrpcSuccessFirstTry() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(retryAttemptNumber, BASELINE_ATTEMPTS);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                assertEquals(BASELINE_ATTEMPTS, retryAttemptNumber.get());
                assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
            }
        }


        @ParameterizedTest
        @EnumSource(value = Status.Code.class, names = {"RESOURCE_EXHAUSTED", "UNAVAILABLE"}, mode = EnumSource.Mode.INCLUDE)
        void testGrpcSuccessAfterRecoverableCodes(Status.Code code) {
            Status status = Status.fromCode(code);

            try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status), retryAttemptNumber, RETRY_ATTEMPTS);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                process(place, o);

                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                assertEquals(RETRY_ATTEMPTS, retryAttemptNumber.get());
                assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
            }
        }

        @ParameterizedTest
        @EnumSource(value = Status.Code.class, names = {"RESOURCE_EXHAUSTED", "UNAVAILABLE"}, mode = EnumSource.Mode.INCLUDE)
        void testGrpcFailureAfterMaxRecoverableCodes(Status.Code code) {
            Status status = Status.fromCode(code);
            int attemptMax = RETRY_ATTEMPTS + 1;

            try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status), retryAttemptNumber, attemptMax);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(RETRY_ATTEMPTS, retryAttemptNumber.get());
                assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
            }
        }

        @ParameterizedTest
        @EnumSource(
                value = Status.Code.class,
                names = {"OK", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"},
                mode = EnumSource.Mode.EXCLUDE)
        void testGrpcFailureAfterNonRecoverableCode(Status.Code code) {
            Status status = Status.fromCode(code);

            try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status), retryAttemptNumber, RETRY_ATTEMPTS);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertTrue(e.getMessage().startsWith(code.name()));
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(1, retryAttemptNumber.get());
                assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
            }
        }

        @Test
        void testGrpcFailureAfterRuntimeExceptions() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(new IllegalStateException("failed"), retryAttemptNumber, RETRY_ATTEMPTS);
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                        new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () -> process(place, o));
                assertEquals(Status.Code.UNKNOWN, e.getStatus().getCode());
                assertEquals(Status.Code.UNKNOWN.name(), e.getMessage());
                assertTrue(o.getAlternateViewNames().isEmpty());
                assertEquals(1, retryAttemptNumber.get());
                assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
            }
        }
    }

    @Nested
    class SequentialProcessingTests {
        @Test
        void testConnectionIsNotValidated() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(false);
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                PoolException exception = assertThrows(PoolException.class, () -> place.processEndpointsSequentially(o));
                assertEquals("Unable to borrow channel from pool: Unable to validate object", exception.getMessage());
            }
        }

        @Test
        void testBlockedResponsesAreProcessedSequentially() throws InterruptedException {
            CountDownLatch startedLatchOne = new CountDownLatch(1);
            CountDownLatch releaseLatchOne = new CountDownLatch(1);
            CountDownLatch startedLatchTwo = new CountDownLatch(1);
            CountDownLatch releaseLatchTwo = new CountDownLatch(1);

            try (GrpcSampleServer serverOne = new GrpcSampleServer(startedLatchOne, releaseLatchOne);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(startedLatchTwo, releaseLatchTwo)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                AtomicReference<Map<String, byte[]>> viewRef = new AtomicReference<>(o.getAlternateViews());
                AtomicReference<Throwable> errorRef = new AtomicReference<>();

                Thread clientThread = new Thread(() -> {
                    try {
                        place.processEndpointsSequentially(o);
                    } catch (Throwable t) {
                        errorRef.set(t);
                    }
                });

                clientThread.start();

                assertTrue(startedLatchOne.await(1, TimeUnit.SECONDS), "First server should have received request");
                assertEquals(1L, startedLatchTwo.getCount(), "Second server should be waiting to receive request");
                assertTrue(viewRef.get().isEmpty());
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatchOne.countDown();

                assertTrue(startedLatchTwo.await(1, TimeUnit.SECONDS));
                assertEquals(1, viewRef.get().size());
                assertArrayEquals(INPUT_DATA, viewRef.get().get(ENDPOINT_1));
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatchTwo.countDown();

                clientThread.join(1000); // Wait for thread to die
                assertFalse(clientThread.isAlive());

                assertEquals(2, viewRef.get().size());
                assertArrayEquals(INPUT_DATA, viewRef.get().get(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, viewRef.get().get(ENDPOINT_2));
                assertNull(errorRef.get());
            }
        }

        @Nested
        class SequentialRetryDisabledTests extends RetryDisabledTests {
            public void process(GrpcSampleServicePlace place, IBaseDataObject data) {
                place.processEndpointsSequentially(data);
            }
        }

        @Nested
        class SequentialRetryEnabledTests extends RetryEnabledTests {
            public void process(GrpcSampleServicePlace place, IBaseDataObject data) {
                place.processEndpointsSequentially(data);
            }
        }
    }

    @Nested
    class ParallelProcessingTests {
        private void process(GrpcSampleServicePlace place, IBaseDataObject data) {
            place.processEndpointsInParallel(data);
        }

        @Test
        void testConnectionIsNotValidated() {
            try (GrpcSampleServer serverOne = new GrpcSampleServer(false);
                    GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                PoolException exception = assertThrows(PoolException.class, () -> process(place, o));
                assertEquals("Unable to borrow channel from pool: Unable to validate object", exception.getMessage());
            }
        }

        @Test
        void testFutureResponsesAreProcessedInParallel() throws InterruptedException {
            CountDownLatch startedLatch = new CountDownLatch(2);
            CountDownLatch releaseLatch = new CountDownLatch(1);

            try (GrpcSampleServer serverOne = new GrpcSampleServer(startedLatch, releaseLatch);
                    GrpcSampleServer serverTwo = new GrpcSampleServer(startedLatch, releaseLatch)) {

                GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo);

                AtomicReference<Map<String, byte[]>> viewRef = new AtomicReference<>(o.getAlternateViews());
                AtomicReference<Throwable> errorRef = new AtomicReference<>();

                Thread clientThread = new Thread(() -> {
                    try {
                        process(place, o);
                    } catch (Throwable t) {
                        errorRef.set(t);
                    }
                });

                clientThread.start();

                assertTrue(startedLatch.await(2, TimeUnit.SECONDS), "Not all servers received requests");
                assertTrue(viewRef.get().isEmpty());
                assertNull(errorRef.get());

                assertTrue(clientThread.isAlive());
                releaseLatch.countDown();

                clientThread.join(1000); // Wait for thread to die
                assertFalse(clientThread.isAlive());

                assertEquals(2, viewRef.get().size());
                assertArrayEquals(INPUT_DATA, viewRef.get().get(ENDPOINT_1));
                assertArrayEquals(INPUT_DATA, viewRef.get().get(ENDPOINT_2));
                assertNull(errorRef.get());
            }
        }

        @Nested
        class ParallelRetryDisabledTests extends GrpcSampleServicePlaceTest.RetryDisabledTests {
            public void process(GrpcSampleServicePlace place, IBaseDataObject data) {
                place.processEndpointsInParallel(data);
            }

            @ParameterizedTest
            @EnumSource(value = Status.Code.class, names = {"OK"}, mode = EnumSource.Mode.EXCLUDE)
            void testGrpcFailureAfterExceptionCodeWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);
                try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status));
                        GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                    GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                            new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                    place.processEndpointsInParallel(t -> SampleResponse.getDefaultInstance(), o);

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                }
            }

            @Test
            void testGrpcFailureAfterRuntimeExceptionWithDefaultResponse() {
                try (GrpcSampleServer serverOne = new GrpcSampleServer(new IllegalStateException("failed"));
                        GrpcSampleServer serverTwo = new GrpcSampleServer()) {

                    GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                            new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                    place.processEndpointsInParallel(t -> SampleResponse.getDefaultInstance(), o);

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                }
            }
        }

        @Nested
        class ParallelRetryEnabledTests extends GrpcSampleServicePlaceTest.RetryEnabledTests {
            public void process(GrpcSampleServicePlace place, IBaseDataObject data) {
                place.processEndpointsInParallel(data);
            }

            @ParameterizedTest
            @EnumSource(value = Status.Code.class, names = {"RESOURCE_EXHAUSTED", "UNAVAILABLE"}, mode = EnumSource.Mode.INCLUDE)
            void testGrpcFailureAfterMaxRecoverableCodesWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);
                int attemptMax = RETRY_ATTEMPTS + 1;

                try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status), retryAttemptNumber, attemptMax);
                        GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                    GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                            new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                    place.processEndpointsInParallel(t -> SampleResponse.getDefaultInstance(), o);

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(RETRY_ATTEMPTS, retryAttemptNumber.get());
                    assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
                }
            }

            @ParameterizedTest
            @EnumSource(
                    value = Status.Code.class,
                    names = {"OK", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED", "UNAVAILABLE"},
                    mode = EnumSource.Mode.EXCLUDE)
            void testGrpcFailureAfterNonRecoverableCodeWithDefaultResponse(Status.Code code) {
                Status status = Status.fromCode(code);

                try (GrpcSampleServer serverOne = new GrpcSampleServer(new StatusRuntimeException(status), retryAttemptNumber, RETRY_ATTEMPTS);
                        GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                    GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                            new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                    place.processEndpointsInParallel(t -> SampleResponse.getDefaultInstance(), o);

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(1, retryAttemptNumber.get());
                    assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
                }
            }

            @Test
            void testGrpcFailureAfterRuntimeExceptionWithDefaultResponse() {
                try (GrpcSampleServer serverOne = new GrpcSampleServer(new IllegalStateException("failed"), retryAttemptNumber, RETRY_ATTEMPTS);
                        GrpcSampleServer serverTwo = new GrpcSampleServer(baselineAttemptNumber, BASELINE_ATTEMPTS)) {

                    GrpcSampleServicePlace place = buildPlaceWithEndpoints(serverOne, serverTwo,
                            new ConfigEntry(RetryHandler.GRPC_RETRY_MAX_ATTEMPTS, Integer.toString(RETRY_ATTEMPTS)));

                    place.processEndpointsInParallel(t -> SampleResponse.getDefaultInstance(), o);

                    assertEquals(0, o.getAlternateView(ENDPOINT_1).length);
                    assertArrayEquals(INPUT_DATA, o.getAlternateView(ENDPOINT_2));
                    assertEquals(1, retryAttemptNumber.get());
                    assertTrue(BASELINE_ATTEMPTS >= baselineAttemptNumber.get());
                }
            }
        }
    }
}
