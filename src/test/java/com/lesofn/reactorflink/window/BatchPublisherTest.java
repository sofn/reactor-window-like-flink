package com.lesofn.reactorflink.window;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author lesofn
 * @version 1.0 Created at: 2021-02-25 20:02
 */
public class BatchPublisherTest {

    @Test
    public void subscribe() throws InterruptedException {
        int itemCount = 50000;
        int threadCount = 10;

        BatchPublisher<String> publisher = new BatchPublisher<>(16, (int) (threadCount * 1.5), Duration.ofMillis(1));

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        AtomicInteger atomicInteger = new AtomicInteger();

        pool.submit(() -> publisher.subscribe(it -> {
            System.out.println(it.size() + "\t" + publisher.getQueueSize() + "\t" + Thread.currentThread());
            atomicInteger.addAndGet(it.size());
        }));

        Flux.range(0, itemCount).subscribe(i -> pool.submit(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                publisher.onNext(i + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        TimeUnit.MILLISECONDS.sleep(itemCount / threadCount * 2);
        System.out.println(atomicInteger.get());
        assertEquals(itemCount, atomicInteger.get());
    }
}