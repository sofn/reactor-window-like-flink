package com.lesofn.reactorflink.window;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.function.Consumer;

/**
 * @author lesofn
 * @version 1.0 Created at: 2021-02-25 19:19
 */
public class BatchPublisher<T> {
    public static final int DEFAULT_QUEUE_SIZE = 1 << 4; //default 16

    private final Queue<T> queue;
    private final Sinks.Many<T> sinks;
    private final Flux<Mono<List<T>>> flux;
    private final int queueCapacity;

    //失败一直重传
    public static final Sinks.EmitFailureHandler ALWAYS_RETRY_HANDLER = (signalType, emitResult) -> emitResult.isFailure();

    /**
     * @param windowMaxBatchSize window聚合最大size
     * @param windowDuration     window聚合最大等待时间
     */
    public BatchPublisher(int windowMaxBatchSize, Duration windowDuration) {
        this(DEFAULT_QUEUE_SIZE, windowMaxBatchSize, windowDuration);
    }

    /**
     * @param windowMaxBatchSize window聚合最大size
     * @param windowDuration     window聚合最大等待时间
     * @param scheduler          schedule运行的线程池
     */
    public BatchPublisher(int windowMaxBatchSize, Duration windowDuration, Scheduler scheduler) {
        this(DEFAULT_QUEUE_SIZE, windowMaxBatchSize, windowDuration, scheduler);
    }

    /**
     * @param queueCapacity      队列容量
     * @param windowMaxBatchSize window聚合最大size
     * @param windowDuration     window聚合最大等待时间
     */
    public BatchPublisher(int queueCapacity, int windowMaxBatchSize, Duration windowDuration) {
        this(queueCapacity, windowMaxBatchSize, windowDuration, Schedulers.parallel());
    }

    /**
     * @param queueCapacity      队列容量
     * @param windowMaxBatchSize window聚合最大size
     * @param windowDuration     window聚合最大等待时间
     * @param scheduler          schedule运行的线程池
     */
    public BatchPublisher(int queueCapacity, int windowMaxBatchSize, Duration windowDuration, Scheduler scheduler) {
        this.queueCapacity = Queues.ceilingNextPowerOfTwo(queueCapacity);
        this.queue = Queues.<T>get(queueCapacity).get();
        this.sinks = Sinks.many().unicast().onBackpressureBuffer(queue);
        flux = sinks.asFlux()
                .subscribeOn(Schedulers.parallel())
                .windowTimeout(windowMaxBatchSize, windowDuration, Schedulers.newSingle("timer"))
                .map(Flux::collectList)
                .map(it -> it.subscribeOn(scheduler));

    }

    /**
     * 异步发送单个数据
     *
     * @param item 数据项
     */
    public void onNext(T item) {
        sinks.emitNext(item, ALWAYS_RETRY_HANDLER);
    }

    /**
     * 异步发送多个数据
     *
     * @param items 集合数据项
     */
    public void onNext(Collection<T> items) {
        for (T item : items) {
            onNext(item);
        }
    }

    /**
     * @return 队列元素数量
     */
    public int getQueueSize() {
        return this.queue.size();
    }

    /**
     * @return 队列容量
     */
    public int getQueueCapacity() {
        return this.queueCapacity;
    }

    /**
     * 订阅
     *
     * @param consumer 消费者
     */
    public void subscribe(Consumer<? super Collection<T>> consumer) {
        this.flux.subscribe(mono -> mono.filter(it -> !it.isEmpty()).subscribe(consumer));
    }

    /**
     * 订阅
     *
     * @param consumer      消费者
     * @param errorConsumer 异常消费者
     */
    public void subscribe(Consumer<? super Collection<T>> consumer, Consumer<? super Throwable> errorConsumer) {
        this.flux.subscribe(mono -> mono.filter(it -> !it.isEmpty()).subscribe(consumer, errorConsumer));
    }
}
