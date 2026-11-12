package com.comphenix.protocol.injector.netty.channel;

import java.util.concurrent.Executor;

/** Keeps later packets behind a main-thread listener until its event-loop continuation has run. */
final class InboundPacketQueue {
    private final Executor eventLoop;
    // Owned by the event loop, including completion of main-thread work.
    private int pending;

    InboundPacketQueue(Executor eventLoop) {
        this.eventLoop = eventLoop;
    }

    boolean hasPending() {
        return this.pending != 0;
    }

    void dispatch(boolean requiresMainThread, Executor mainThread, Runnable process) {
        if (!requiresMainThread && !this.hasPending()) {
            process.run();
            return;
        }
        this.pending++;
        try {
            mainThread.execute(() -> {
                try {
                    process.run();
                } finally {
                    // Enqueued after the packet's forwarding task, never released on the main thread.
                    this.eventLoop.execute(() -> this.pending--);
                }
            });
        } catch (RuntimeException exception) {
            this.pending--;
            throw exception;
        }
    }
}
