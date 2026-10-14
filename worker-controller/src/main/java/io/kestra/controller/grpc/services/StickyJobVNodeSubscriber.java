package io.kestra.controller.grpc.services;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.queues.KeyedDispatchQueueInterface;
import io.kestra.core.queues.QueueException;
import io.kestra.core.queues.QueueSubscriber;
import io.kestra.core.runners.WorkerJobEvent;
import io.kestra.core.runners.WorkerJobEvent.StickyRouting;
import io.kestra.core.server.ServiceLivenessStore;
import io.kestra.core.utils.Disposable;
import io.kestra.core.utils.Either;
import io.kestra.core.vnodes.ControllerVNodesAssignmentChannel;
import io.kestra.core.vnodes.VNodesAssigner;
import io.kestra.core.vnodes.internals.DefaultVNodesAssigner;
import io.kestra.core.worker.StickyWorkerRouting;

import io.micronaut.context.annotation.Requires;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import lombok.extern.slf4j.Slf4j;

/**
 * Subscribes this controller to the vNodes the liveness coordinator assigns it, and dispatches the sticky jobs received
 * on them, forwarding the ones whose worker is held by another controller.
 */
@Singleton
@Slf4j
@Requires(beans = ServiceLivenessStore.class)
public class StickyJobVNodeSubscriber implements WorkerLifecycleListener {

    private final KeyedDispatchQueueInterface<WorkerJobEvent> workerJobEventQueue;
    private final StickyWorkerRouting routing;
    private final DefaultVNodesAssigner vNodesAssigner;
    private final Map<Integer, QueueSubscriber<WorkerJobEvent>> subscribers = new ConcurrentHashMap<>();

    private volatile WorkerJobDispatcher dispatcher;
    private volatile Disposable assignment;

    @Inject
    public StickyJobVNodeSubscriber(KeyedDispatchQueueInterface<WorkerJobEvent> workerJobEventQueue,
        StickyWorkerRouting routing,
        ControllerVNodesAssignmentChannel channel) {
        this.workerJobEventQueue = workerJobEventQueue;
        this.routing = routing;
        this.vNodesAssigner = new DefaultVNodesAssigner(channel);
    }

    @Override
    public void init(WorkerJobDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        vNodesAssigner.start();
    }

    /**
     * Joins the vNodes assignment as the given controller.
     */
    public void start(String controllerId) {
        assignment = vNodesAssigner.subscribe(controllerId, new VNodesAssigner.VNodeAssignmentListener() {
            @Override
            public void onVNodesRevoked() {
                subscribers.keySet().stream().toList().forEach(vNode -> closeQuietly(vNode, subscribers.remove(vNode)));
            }

            @Override
            public void onVNodesAssigned(Set<Integer> vNodes) {
                vNodes.forEach(vNode -> subscribers.computeIfAbsent(vNode, StickyJobVNodeSubscriber.this::subscribe));
            }
        });
    }

    /**
     * @return the vNodes this controller is assigned, published with its liveness.
     */
    public Set<Integer> assignedVNodes() {
        return Set.copyOf(subscribers.keySet());
    }

    private QueueSubscriber<WorkerJobEvent> subscribe(int vNode) {
        QueueSubscriber<WorkerJobEvent> subscriber = workerJobEventQueue.subscriber(StickyWorkerRouting.routingKey(vNode));
        subscriber.subscribe(this::handle);
        log.info("Subscribed to vNode {} for sticky jobs", vNode);
        return subscriber;
    }

    private void handle(Either<WorkerJobEvent, DeserializationException> either) {
        if (either.isRight()) {
            log.error("Deserialization error for a sticky job: {}", either.getRight().getMessage());
            return;
        }
        WorkerJobEvent event = either.getLeft();
        if (!dispatcher.dispatchSticky(event)) {
            forward(event);
        }
    }

    private void forward(WorkerJobEvent event) {
        StickyRouting stickyRouting = event.stickyRouting();
        try {
            Optional<String> routingKey = routing.routingKeyForWorker(stickyRouting.workerId());
            if (routingKey.isPresent()) {
                WorkerJobEvent forwarded = event.withStickyRouting(stickyRouting.withVNodeRoutingKey(routingKey.get()));
                workerJobEventQueue.emit(forwarded.key(), forwarded);
            } else if (routing.isWorkerLost(stickyRouting.workerId())) {
                log.warn(
                    "Dropping the job {}: its worker '{}' is lost, so the liveness coordinator recovers it.",
                    event.job().uid(), stickyRouting.workerId()
                );
            } else {
                workerJobEventQueue.emit(event.key(), event);
            }
        } catch (QueueException e) {
            log.error("Failed to forward the sticky job {}: {}", event.job().uid(), e.getMessage(), e);
        }
    }

    private void closeQuietly(int vNode, QueueSubscriber<WorkerJobEvent> subscriber) {
        try {
            subscriber.close();
            log.info("Unsubscribed from vNode {} for sticky jobs", vNode);
        } catch (Exception e) {
            log.warn("Error closing the subscription to vNode {}: {}", vNode, e.getMessage());
        }
    }

    @PreDestroy
    public void close() {
        if (assignment != null) {
            assignment.dispose();
        }
        vNodesAssigner.stop();
        subscribers.forEach(this::closeQuietly);
        subscribers.clear();
    }
}
