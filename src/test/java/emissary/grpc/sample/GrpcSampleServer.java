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

    public GrpcSampleServer() {
        this(true);
    }

    public GrpcSampleServer(RuntimeException processingException) {
        this(request -> {
            throw processingException;
        });
    }

    public GrpcSampleServer(AtomicInteger attemptNumber, int attemptMax) {
        this(request -> {
            if (attemptNumber.getAndIncrement() < attemptMax) {
                return request.getQuery();
            }
            throw new IllegalStateException("Surpassed max attempts");
        });
    }

    public GrpcSampleServer(RuntimeException processingException, AtomicInteger attemptNumber, int attemptMax) {
        this(request -> {
            if (attemptNumber.incrementAndGet() < attemptMax) {
                throw processingException;
            }
            return request.getQuery();
        });
    }

    public GrpcSampleServer(CountDownLatch startedLatch, CountDownLatch releaseLatch) {
        this(request -> {
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

    public GrpcSampleServer(Function<SampleRequest, ByteString> processor) {
        this(true, processor);
    }

    public GrpcSampleServer(boolean health) {
        this(health, SampleRequest::getQuery);
    }

    public GrpcSampleServer(boolean health, Function<SampleRequest, ByteString> processor) {
        this(new SampleServiceGrpc.SampleServiceImplBase() {
            @Override
            public void callSampleHealthCheck(Empty request, StreamObserver<SampleHealthStatus> responseObserver) {
                SampleHealthStatus status = SampleHealthStatus.newBuilder().setOk(health).build();
                responseObserver.onNext(status);
                responseObserver.onCompleted();
            }

            @Override
            public void callSampleService(SampleRequest request, StreamObserver<SampleResponse> responseObserver) {
                try {
                    SampleResponse response = SampleResponse.newBuilder()
                            .setResult(processor.apply(request))
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                } catch (Throwable t) {
                    responseObserver.onError(Status.fromThrowable(t).asRuntimeException());
                }
            }
        });
    }

    public GrpcSampleServer(BindableService service) {
        try {
            server = ServerBuilder.forPort(0)
                    .addService(service)
                    .build()
                    .start();
        } catch (IOException e) {
            throw new IllegalStateException("Problem starting server: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        server.shutdownNow();
    }

    public String getPort() {
        return String.valueOf(server.getPort());
    }
}
