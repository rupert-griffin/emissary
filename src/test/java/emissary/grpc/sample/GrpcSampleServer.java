package emissary.grpc.sample;

import emissary.grpc.sample.v1.SampleHealthStatus;
import emissary.grpc.sample.v1.SampleRequest;
import emissary.grpc.sample.v1.SampleResponse;
import emissary.grpc.sample.v1.SampleServiceGrpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class GrpcSampleServer implements AutoCloseable {
    private final Server server;

    private GrpcSampleServer(BindableService service) {
        try {
            server = ServerBuilder.forPort(0)
                    .addService(service)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException("Problem starting server: " + e.getMessage(), e);
        }
    }

    private static SampleServiceGrpc.SampleServiceImplBase newService(Function<SampleRequest, ByteString> behavior, boolean healthOk) {
        return new SampleServiceGrpc.SampleServiceImplBase() {
            @Override
            public void callSampleHealthCheck(Empty request, StreamObserver<SampleHealthStatus> responseObserver) {
                SampleHealthStatus status = SampleHealthStatus.newBuilder().setOk(healthOk).build();
                responseObserver.onNext(status);
                responseObserver.onCompleted();
            }

            @Override
            public void callSampleService(SampleRequest request, StreamObserver<SampleResponse> responseObserver) {
                try {
                    SampleResponse response = SampleResponse.newBuilder()
                            .setResult(behavior.apply(request))
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Throwable t) {
                    responseObserver.onError(Status.fromThrowable(t).asRuntimeException());
                }
            }
        };
    }

    public static GrpcSampleServer defaultBehavior() {
        return GrpcSampleServer.of(true);
    }

    public static GrpcSampleServer of(boolean healthOk) {
        return GrpcSampleServer.of(SampleRequest::getQuery, healthOk);
    }

    public static GrpcSampleServer of(Function<SampleRequest, ByteString> behavior) {
        return GrpcSampleServer.of(behavior, true);
    }

    public static GrpcSampleServer of(Function<SampleRequest, ByteString> behavior, boolean healthOk) {
        return new GrpcSampleServer(newService(behavior, healthOk));
    }

    public static GrpcSampleServer alwaysThrow(RuntimeException ex) {
        return GrpcSampleServer.of(request -> {
            throw ex;
        });
    }

    public static GrpcSampleServer throwAfter(int maxAttempts, AtomicInteger counter, RuntimeException ex) {
        return GrpcSampleServer.of(request -> {
            if (counter.getAndIncrement() < maxAttempts) {
                return request.getQuery();
            }
            throw ex;
        });
    }

    public static GrpcSampleServer throwUntil(int maxAttempts, AtomicInteger counter, RuntimeException ex) {
        return GrpcSampleServer.of(request -> {
            if (counter.incrementAndGet() < maxAttempts) {
                throw ex;
            }
            return request.getQuery();
        });
    }

    public static GrpcSampleServer blockUntilReleased(CountDownLatch startedLatch, CountDownLatch releaseLatch) {
        return GrpcSampleServer.of(request -> {
            startedLatch.countDown();
            try {
                releaseLatch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            return request.getQuery();
        });
    }

    @Override
    public void close() {
        server.shutdownNow();
    }

    public String getPort() {
        return String.valueOf(server.getPort());
    }
}
