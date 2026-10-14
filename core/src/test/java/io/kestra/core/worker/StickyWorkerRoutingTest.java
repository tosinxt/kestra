package io.kestra.core.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.kestra.core.runners.WorkerJob;
import io.kestra.core.runners.WorkerJobEvent;
import io.kestra.core.server.Service;
import io.kestra.core.server.ServiceInstance;
import io.kestra.core.server.ServiceLivenessStore;
import io.kestra.core.server.ServiceType;
import io.kestra.core.vnodes.ControllerVNodesConfiguration;
import io.kestra.core.vnodes.VNodes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StickyWorkerRoutingTest {

    private final ServiceLivenessStore store = mock(ServiceLivenessStore.class);
    private final StickyWorkerRouting routing = new StickyWorkerRouting(store, new ControllerVNodesConfiguration(16, Duration.ofSeconds(5)));

    @Test
    void shouldRouteToAVNodeAssignedToTheControllerHoldingTheWorker() {
        when(store.findAllInstancesInState(Service.ServiceState.RUNNING)).thenReturn(
            List.of(
                controller("controller-a", List.of("worker-1"), List.of(0, 1)),
                controller("controller-b", List.of("worker-2"), List.of(7, 3))
            )
        );

        WorkerJobEvent event = routing.route(mock(WorkerJob.class), null, "worker-2");

        assertThat(event.stickyRouting().workerId()).isEqualTo("worker-2");
        assertThat(event.key()).isEqualTo(StickyWorkerRouting.routingKey(3));
    }

    @Test
    void shouldRouteToTheWorkerVNodeWhenNoControllerHoldsTheWorker() {
        when(store.findAllInstancesInState(Service.ServiceState.RUNNING)).thenReturn(
            List.of(controller("controller-a", List.of("worker-1"), List.of(0, 1)))
        );

        WorkerJobEvent event = routing.route(mock(WorkerJob.class), null, "worker-2");

        assertThat(event.key()).isEqualTo(StickyWorkerRouting.routingKey(VNodes.computeVNode(16, "worker-2")));
    }

    private static ServiceInstance controller(String id, List<String> workerIds, List<Integer> vNodes) {
        return new ServiceInstance(
            id, ServiceType.CONTROLLER, Service.ServiceState.RUNNING, null, Instant.now(), Instant.now(), List.of(), null,
            Map.of(StickyWorkerRouting.WORKERS_PROP, workerIds, StickyWorkerRouting.VNODES_PROP, vNodes), Set.of()
        );
    }
}
