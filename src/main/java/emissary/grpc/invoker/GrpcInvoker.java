package emissary.grpc.invoker;

import emissary.grpc.channel.ChannelManager;
import emissary.grpc.exceptions.GrpcExceptionUtils;
import emissary.grpc.retry.RetryHandler;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.GeneratedMessageV3;
import io.grpc.ManagedChannel;
import io.grpc.stub.AbstractBlockingStub;
import io.grpc.stub.AbstractFutureStub;
import jakarta.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.BiFunction;
import java.util.function.Function;

public class GrpcInvoker implements AutoCloseable {
    private final ChannelManager channelManager;
    private final RetryHandler retryHandler;

    public GrpcInvoker(ChannelManager channelManager, RetryHandler retryHandler) {
        this.channelManager = channelManager;
        this.retryHandler = retryHandler;
    }

    /**
     * Executes a unary gRPC call to a given endpoint using an {@link AbstractBlockingStub}. If the gRPC connection fails
     * due to an allowed Exception, the call will be tried again per the configurations set using {@link RetryHandler}. All
     * other Exceptions are thrown on the spot. Will also throw an Exception once max attempts have been reached.
     *
     * @param stubFactory function that creates the appropriate gRPC stub from a {@link ManagedChannel}
     * @param callLogic function that performs the actual gRPC call using the stub and request
     * @param request the protobuf request message to send
     * @return the response returned by the gRPC call
     * @param <Q> the protobuf request type
     * @param <R> the protobuf response type
     * @param <S> the gRPC stub type
     */
    public <Q extends GeneratedMessageV3, R extends GeneratedMessageV3, S extends AbstractBlockingStub<S>> R invoke(
            Function<ManagedChannel, S> stubFactory, BiFunction<S, Q, R> callLogic, Q request) {
        return retryHandler.execute(() -> {
            ManagedChannel channel = channelManager.acquire();
            try {
                S stub = stubFactory.apply(channel);
                R response = callLogic.apply(stub, request);
                channelManager.release(channel);
                return response;
            } catch (RuntimeException e) {
                channelManager.shutdown(channel);
                throw GrpcExceptionUtils.toContextualRuntimeException(e);
            }
        });
    }

    /**
     * Executes a unary gRPC call to a given endpoint using an {@link AbstractFutureStub}. If the gRPC connection fails due
     * to an allowed Exception, the call will be tried again per the configurations set using {@link RetryHandler}. All
     * other Exceptions are thrown on the spot. Will also throw an Exception once max attempts have been reached.
     *
     * @param stubFactory function that creates the appropriate gRPC stub from a {@link ManagedChannel}
     * @param callLogic function that performs the actual gRPC call using the stub and request
     * @param request the protobuf request message to send
     * @return the future that waits for the response returned by the gRPC call
     * @param <Q> the protobuf request type
     * @param <R> the protobuf response type
     * @param <S> the gRPC stub type
     */
    public <Q extends GeneratedMessageV3, R extends GeneratedMessageV3, S extends AbstractFutureStub<S>> CompletableFuture<R> invokeAsync(
            Function<ManagedChannel, S> stubFactory, BiFunction<S, Q, ListenableFuture<R>> callLogic, Q request) {
        return retryHandler.executeAsync(() -> {
            ManagedChannel channel = channelManager.acquire();
            try {
                S stub = stubFactory.apply(channel);
                return handleFuture(callLogic.apply(stub, request), channel);
            } catch (RuntimeException e) {
                channelManager.shutdown(channel);
                throw GrpcExceptionUtils.toContextualRuntimeException(e);
            }
        }).toCompletableFuture();
    }

    /**
     * Adds a handler for gRPC future completion that manages channel lifecycle and exception mapping. Runs when the future
     * completes regardless of success or failure.
     *
     * @param channel the borrowed channel
     * @param <R> response type
     * @return handler suitable for {@link CompletableFuture#handle}
     */
    private <R extends GeneratedMessageV3> CompletionStage<R> handleFuture(ListenableFuture<R> listenable, ManagedChannel channel) {
        return toCompletableFuture(listenable)
                .handle((response, throwable) -> {
                    if (throwable == null) {
                        channelManager.release(channel);
                        return response;
                    }
                    channelManager.shutdown(channel);
                    throw GrpcExceptionUtils.toContextualRuntimeException(
                            GrpcExceptionUtils.unwrapAsyncThrowable(throwable));
                });
    }

    /**
     * Converts a Guava future to a Java completable future.
     *
     * @param future Guava future
     * @return completable future
     * @param <T> result type
     */
    private static <T> CompletableFuture<T> toCompletableFuture(ListenableFuture<T> future) {
        CompletableFuture<T> completableFuture = new CompletableFuture<>();
        Futures.addCallback(future, new FutureCallback<>() {
            @Override
            public void onSuccess(T result) {
                completableFuture.complete(result);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                completableFuture.completeExceptionally(t);
            }
        }, MoreExecutors.directExecutor()); // Direct executor runs callback immediately on thread completing Guava future
        return completableFuture;
    }

    public String getHost() {
        return channelManager.getHost();
    }

    public int getPort() {
        return channelManager.getPort();
    }

    @Override
    public void close() {
        channelManager.close();
    }
}
