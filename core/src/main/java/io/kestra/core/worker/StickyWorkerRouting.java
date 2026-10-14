package io.kestra.core.worker;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;

import io.kestra.core.runners.WorkerJob;
import io.kestra.core.runners.WorkerJobEvent;
import io.kestra.core.server.Service;
import io.kestra.core.server.ServiceInstance;
import io.kestra.core.server.ServiceLivenessStore;
import io.kestra.core.server.ServiceType;
import io.kestra.core.vnodes.ControllerVNodesConfiguration;
import io.kestra.core.vnodes.VNodes;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Routes a worker job to one specific worker through a vNode of the controller holding it.
 * <p>
 * The controllers share vNodes assigned by the vNodes assignment protocol, and each one publishes in its liveness
 * properties the vNodes it was assigned ({@link #VNODES_PROP}) and the workers it holds ({@link #WORKERS_PROP}). A view
 * older than a rebalance or a reconnection only costs a forward from the controller that received the job.
 */
@Singleton
public class StickyWorkerRouting {

    public static final String WORKERS_PROP = "workers";
    public static final String VNODES_PROP = "vnodes";

    private static final String VNODE_ROUTING_KEY_PREFIX = "__vnode-";

    private final ServiceLivenessStore serviceLivenessStore;
    private final int vNodeCount;

    @Inject
    public StickyWorkerRouting(ServiceLivenessStore serviceLivenessStore, ControllerVNodesConfiguration configuration) {
        this.serviceLivenessStore = serviceLivenessStore;
        this.vNodeCount = configuration.count();
    }

    public static String routingKey(int vNode) {
        return VNODE_ROUTING_KEY_PREFIX + vNode;
    }

    /**
     * Builds the event sending the given job to the given worker. When no running controller is known to hold the
     * worker, the job goes to a vNode derived from the worker id, whose controller forwards it once the worker is found.
     */
    public WorkerJobEvent route(WorkerJob job, String workerQueueId, String workerId) {
        String routingKey = routingKeyForWorker(workerId)
            .orElseGet(() -> routingKey(VNodes.computeVNode(vNodeCount, workerId)));
        return new WorkerJobEvent(workerQueueId, job, new WorkerJobEvent.StickyRouting(workerId, routingKey));
    }

    public Optional<String> routingKeyForWorker(String workerId) {
        return serviceLivenessStore.findAllInstancesInState(Service.ServiceState.RUNNING)
            .stream()
            .filter(instance -> instance.is(ServiceType.CONTROLLER))
            .filter(controller -> prop(controller, WORKERS_PROP).contains(workerId))
            .flatMap(controller -> prop(controller, VNODES_PROP).stream())
            .map(vNode -> ((Number) vNode).intValue())
            .min(Integer::compareTo)
            .map(StickyWorkerRouting::routingKey);
    }

    /**
     * A worker is lost once the liveness coordinator has declared it {@link Service.ServiceState#INACTIVE}, or has
     * removed it: from then on its jobs are recovered by the coordinator, not delivered.
     */
    public boolean isWorkerLost(String workerId) {
        return serviceLivenessStore.findAllInstancesInStates(EnumSet.complementOf(EnumSet.of(Service.ServiceState.INACTIVE)))
            .stream()
            .noneMatch(instance -> instance.uid().equals(workerId));
    }

    private static Collection<?> prop(ServiceInstance controller, String name) {
        if (controller.props() != null && controller.props().get(name) instanceof Collection<?> values) {
            return values;
        }
        return List.of();
    }
}
