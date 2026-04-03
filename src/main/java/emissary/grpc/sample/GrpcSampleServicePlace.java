package emissary.grpc.sample;

import emissary.config.Configurator;
import emissary.core.IBaseDataObject;
import emissary.grpc.GrpcRoutingPlace;
import emissary.grpc.invoker.AsyncInvoker;
import emissary.grpc.sample.v1.SampleRequest;
import emissary.grpc.sample.v1.SampleResponse;
import emissary.grpc.sample.v1.SampleServiceGrpc;
import emissary.grpc.sample.v1.SampleServiceGrpc.SampleServiceBlockingStub;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Connects to an arbitrary external service that implements {@link emissary.grpc.sample.v1.SampleServiceGrpc}.
 */
public class GrpcSampleServicePlace extends GrpcRoutingPlace {
    public GrpcSampleServicePlace(Configurator configs) throws IOException {
        super(configs);
    }

    protected SampleRequest generateRequest(IBaseDataObject o) {
        return SampleRequest.newBuilder()
                .setQuery(ByteString.copyFrom(o.data()))
                .build();
    }

    public void processEndpointsSequentially(IBaseDataObject o) {
        hostnameTable.keySet().stream()
                .sorted(Comparator.naturalOrder())
                .forEach(endpoint -> processEndpointsSequentially(endpoint, o));
    }

    public void processEndpointsSequentially(String endpoint, IBaseDataObject o) {
        SampleResponse response = invokeGrpc(
                endpoint, SampleServiceGrpc::newBlockingStub, SampleServiceBlockingStub::callSampleService, generateRequest(o));

        o.addAlternateView(endpoint, response.getResult().toByteArray());
    }

    public void processEndpointsInParallel(IBaseDataObject o) {
        processEndpointsInParallel(null, o);
    }

    public void processEndpointsInParallel(Function<Throwable, SampleResponse> exceptionally, IBaseDataObject o) {
        Map<String, CompletableFuture<SampleResponse>> futureMap = hostnameTable.keySet().stream()
                .collect(Collectors.toMap(k -> k, k -> invokeGrpcAsync(
                        k,
                        SampleServiceGrpc::newFutureStub,
                        SampleServiceGrpc.SampleServiceFutureStub::callSampleService,
                        generateRequest(o))));
        Map<String, SampleResponse> responseMap = AsyncInvoker.awaitAllAndGet(futureMap, HashMap::new, exceptionally);
        responseMap.forEach((k, v) -> o.addAlternateView(k, v.getResult().toByteArray()));
    }

    /**
     * Calls a health check to the external service before borrowing the channel from the pool.
     *
     * @param managedChannel the gRPC channel to validate
     * @return {@code true} if channel is healthy, otherwise {@code false}
     */
    @Override
    protected boolean validateConnection(ManagedChannel managedChannel) {
        SampleServiceBlockingStub stub = SampleServiceGrpc.newBlockingStub(managedChannel);
        return stub.callSampleHealthCheck(Empty.getDefaultInstance()).getOk();
    }
}
