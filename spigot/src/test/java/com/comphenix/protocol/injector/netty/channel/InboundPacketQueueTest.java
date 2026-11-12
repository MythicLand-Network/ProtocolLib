package com.comphenix.protocol.injector.netty.channel;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class InboundPacketQueueTest {
    @Test
    void movementAndUnobservedPacketsStayBehindSynchronousInteractions() {
        Queue<Runnable> main = new ArrayDeque<>();
        Queue<Runnable> loop = new ArrayDeque<>();
        List<String> received = new ArrayList<>();
        InboundPacketQueue queue = new InboundPacketQueue(loop::add);
        Executor mainThread = main::add;
        queue.dispatch(true, mainThread, () -> loop.add(() -> received.add("target")));
        queue.dispatch(false, mainThread, () -> loop.add(() -> received.add("movement")));
        queue.dispatch(false, mainThread, () -> loop.add(() -> received.add("unobserved")));
        while (!main.isEmpty()) main.remove().run();
        assertTrue(queue.hasPending(), "Main-thread completion must not release the event-loop barrier");
        queue.dispatch(false, mainThread, () -> loop.add(() -> received.add("air-use")));
        while (!main.isEmpty()) main.remove().run();
        while (!loop.isEmpty()) loop.remove().run();
        assertEquals(List.of("target", "movement", "unobserved", "air-use"), received);
        assertFalse(queue.hasPending());
        queue.dispatch(false, mainThread, () -> received.add("fast-movement"));
        assertTrue(main.isEmpty(), "Ordinary movement must not incur a main-thread listener task");
        assertEquals("fast-movement", received.get(4));
    }

    @Test
    void cancellationFailureAndOtherConnectionsDoNotLeaveABarrier() {
        Queue<Runnable> main = new ArrayDeque<>();
        Queue<Runnable> loop = new ArrayDeque<>();
        InboundPacketQueue queue = new InboundPacketQueue(loop::add);
        InboundPacketQueue other = new InboundPacketQueue(loop::add);
        queue.dispatch(true, main::add, () -> { throw new IllegalStateException("listener failed"); });
        assertFalse(other.hasPending());
        assertThrows(IllegalStateException.class, () -> main.remove().run());
        loop.remove().run();
        assertFalse(queue.hasPending());
        queue.dispatch(true, main::add, () -> { }); // Cancelled or disconnected before forwarding.
        main.remove().run();
        loop.remove().run();
        assertFalse(queue.hasPending());
    }
}
