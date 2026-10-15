package io.kestra.controller.grpc.services;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.queues.KeyedDispatchQueueInterface;
import io.kestra.core.queues.QueueSubscriber;
import io.kestra.core.runners.WorkerJob;
import io.kestra.core.runners.WorkerJobEvent;
import io.kestra.core.server.ServiceType;
import io.kestra.core.utils.Disposable;
import io.kestra.core.utils.Either;
import io.kestra.core.vnodes.ControllerVNodesAssignmentChannel;
import io.kestra.core.vnodes.VNodesAssignmentEvent;
import io.kestra.core.worker.StickyWorkerRouting;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StickyJobVNodeSubscriberTest {

    private static final WorkerJobEvent BUSY_WORKER_JOB = new WorkerJobEvent(
        null,
        mock(WorkerJob.class),
        new WorkerJobEvent.StickyRouting("worker-1", StickyWorkerRouting.routingKey(3))
    );

    private final WorkerJobDispatcher dispatcher = mock(WorkerJobDispatcher.class);
    private final QueueSubscriber<WorkerJobEvent> vNodeSubscriber = mock(QueueSubscriber.class);
    private final StickyJobVNodeSubscriber subscriber = subscriber();

    private Consumer<Either<WorkerJobEvent, DeserializationException>> vNodeConsumer;

    @BeforeEach
    void assignVNode() {
        ArgumentCaptor<Consumer<Either<WorkerJobEvent, DeserializationException>>> consumer = ArgumentCaptor.captor();
        verify(vNodeSubscriber).subscribe(consumer.capture());
        vNodeConsumer = consumer.getValue();
        when(dispatcher.dispatchSticky(BUSY_WORKER_JOB)).thenReturn(WorkerJobDispatcher.StickyDispatchResult.WORKER_BUSY);
    }

    @Test
    void shouldPauseTheVNodeOfABusyWorkerUntilItFreesAPermit() {
        when(dispatcher.isWorkerBusy("worker-1", null)).thenReturn(true);

        vNodeConsumer.accept(Either.left(BUSY_WORKER_JOB));
        subscriber.onWorkerCapacityChanged(worker("worker-2"));

        verify(vNodeSubscriber).pause();
        verify(vNodeSubscriber, never()).resume();

        when(dispatcher.isWorkerBusy("worker-1", null)).thenReturn(false);
        subscriber.onWorkerCapacityChanged(worker("worker-1"));

        verify(vNodeSubscriber).resume();
    }

    @Test
    void shouldResumeTheVNodeAtOnceWhenTheWorkerFreedAPermitBeforeThePause() {
        when(dispatcher.isWorkerBusy("worker-1", null)).thenReturn(false);

        vNodeConsumer.accept(Either.left(BUSY_WORKER_JOB));

        InOrder inOrder = inOrder(vNodeSubscriber);
        inOrder.verify(vNodeSubscriber).pause();
        inOrder.verify(vNodeSubscriber).resume();
    }

    private StickyJobVNodeSubscriber subscriber() {
        KeyedDispatchQueueInterface<WorkerJobEvent> queue = mock(KeyedDispatchQueueInterface.class);
        when(queue.subscriber(StickyWorkerRouting.routingKey(3))).thenReturn(vNodeSubscriber);
        ControllerVNodesAssignmentChannel channel = mock(ControllerVNodesAssignmentChannel.class);
        when(channel.serviceType()).thenReturn(ServiceType.CONTROLLER);
        ArgumentCaptor<Consumer<VNodesAssignmentEvent>> assignments = ArgumentCaptor.captor();
        when(channel.subscribe(assignments.capture())).thenReturn(Disposable.of(() -> {}));

        StickyJobVNodeSubscriber stickyJobVNodeSubscriber = new StickyJobVNodeSubscriber(queue, mock(StickyWorkerRouting.class), channel);
        stickyJobVNodeSubscriber.init(dispatcher);
        stickyJobVNodeSubscriber.start("controller-1");

        Instant epoch = Instant.now();
        assignments.getValue().accept(new VNodesAssignmentEvent.VNodesAssignmentRequest(epoch, ServiceType.CONTROLLER, "leader", epoch, Set.of("controller-1")));
        assignments.getValue().accept(new VNodesAssignmentEvent.VNodesAssignmentRelease(epoch, ServiceType.CONTROLLER, "leader", epoch, Map.of("controller-1", Set.of(3))));
        return stickyJobVNodeSubscriber;
    }

    private static WorkerStreamContext<?> worker(String workerId) {
        WorkerStreamContext<?> context = mock(WorkerStreamContext.class);
        when(context.getWorkerId()).thenReturn(workerId);
        return context;
    }
}
