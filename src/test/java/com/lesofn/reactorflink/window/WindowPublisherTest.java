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
public class WindowPublisherTest {

    @Test
    public void testWindowPublisher() throws InterruptedException {
        int itemCount = 50000;
        int threadCount = 10;

        WindowPublisher<String> publisher = new WindowPublisher<>(16, (int) (threadCount * 1.5), Duration.ofMillis(1));

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        AtomicInteger atomicInteger = new AtomicInteger();

        pool.submit(() -> publisher.subscribe(it -> {
            System.out.println(it.size() + "\t" + publisher.getQueueSize() + "\t" + Thread.currentThread());
            atomicInteger.addAndGet(it.size());
        }));

        Flux.range(0, itemCount).subscribe(i -> pool.submit(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1);
                publisher.publish(i + "");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        TimeUnit.MILLISECONDS.sleep(itemCount / threadCount * 2);
        System.out.println(atomicInteger.get());
        assertEquals(itemCount, atomicInteger.get());
    }
}