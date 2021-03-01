用ProjectReactor实现类似Flink window的功能  

### 一、背景
Flink在处理流式任务的时候有很大的优势，其中windows等操作符可以很方便的完成聚合任务，但是Flink是一套独立的服务，业务流程中如果想使用需要将数据发到kafka，用Flink处理完再发到kafka，然后再做业务处理，流程很繁琐。

比如在业务代码中想要实现类似Flink的window按时间批量聚合功能，如果纯手动写代码比较繁琐，使用Flink又太重，这种场景下使用响应式编程RxJava、Reactor等的window、buffer操作符可以很方便的实现。

响应式编程框架也早已有了背压以及丰富的操作符支持，能不能用响应式编程框架处理类似Flink的操作呢，答案是肯定的。

本文使用Reactor来实现Flink的window功能来举例，其他操作符理论上相同。

### 二、实现过程

Flink对流式处理做的很好的封装，使用Flink的时候几乎不用关心线程池、积压、数据丢失等问题，但是使用Reactor实现类似的功能就必须对Reactor运行原理比较了解，并且经过不同场景下测试，否则很容易出问题。

下面列举出实现过程中的核心点：

#### 1、创建Flux和发送数据分离

入门Reactor的时候给的示例都是创建Flux的时候同时就把数据赋值了，比如：Flux.just、Flux.range等，从3.4.0版本后先创建Flux，再发送数据可使用Sinks完成。有两个比较容易混淆的方法：

- Sinks.many().multicast() 如果没有订阅者，那么接收的消息直接丢弃
- Sinks.many().unicast() 如果没有订阅者，那么保存接收的消息直到第一个订阅者订阅
- Sinks.many().replay() 不管有多少订阅者，都保存所有消息

在此示例场景中，选择的是Sinks.many().unicast()

官方文档：https://projectreactor.io/docs/core/release/reference/#processors

#### 2、背压支持

上面方法的对象背压策略支持两种：BackpressureBuffer、BackpressureError，在此场景肯定是选择BackpressureBuffer，需要指定缓存队列，初始化方法如下：Queues.<String>get(queueSize).get()

数据提交有两个方法：

- emitNext 指定提交失败策略同步提交
- tryEmitNext 异步提交，返回提交成功、失败状态

在此场景我们不希望丢数据，可自定义失败策略，提交失败无限重试，当然也可以调用异步方法自己重试。

```
 Sinks.EmitFailureHandler ALWAYS_RETRY_HANDLER = (signalType, emitResult) -> emitResult.isFailure();
```

在此之后就就可以调用Sinks.asFlux开心的使用各种操作符了。

#### 3、窗口函数

Reactor支持两类窗口聚合函数：

- window类：返回Mono(Flux<T>)
- buffer类：返回List<T>

在此场景中，使用buffer即可满足需求，bufferTimeout(int maxSize, Duration maxTime)支持最大个数，最大等待时间操作，Flink中的keys操作可以用groupBy、collectMap来实现。

#### 4、消费者处理

Reactor经过buffer后是一个一个的发送数据，如果使用publishOn或subscribeOn处理的话，只等待下游的subscribe处理完成才会重新request新的数据，buffer操作符才会重新发送数据。如果此时subscribe消费者耗时较长，数据流会在buffer流程阻塞，显然并不是我们想要的。

理想的操作是消费者在一个线程池里操作，可多线程并行处理，如果线程池满，再阻塞buffer操作符。解决方案是自定义一个线程池，并且当然线程池如果任务满submit支持阻塞，可以用自定义RejectedExecutionHandler来实现：

```
 RejectedExecutionHandler executionHandler = (r, executor) -> {
     try {
         executor.getQueue().put(r);
     } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RejectedExecutionException("Producer thread interrupted", e);
     }
 };
 
 new ThreadPoolExecutor(poolSize, poolSize,
         0L, TimeUnit.MILLISECONDS,
         new SynchronousQueue<>(),
         executionHandler);
```

### 三、总结

#### 1、总结一下整体的执行流程

1. 提交任务：提交数据支持同步异步两种方式，支持多线程提交，正常情况下响应很快，同步的方法如果队列满则阻塞。
2. 丰富的操作符处理流式数据。
3. buffer操作符产生的数据多线程处理：同步提交到单独的消费者线程池，线程池任务满则阻塞。
4. 消费者线程池：支持阻塞提交，保证不丢消息，同时队列长度设置成0，因为前面已经有队列了。
5. 背压：消费者线程池阻塞后，会背压到buffer操作符，并背压到缓冲队列，缓存队列满背压到数据提交者。

#### 2、和Flink的对比

实现的Flink的功能：

- 不输Flink的丰富操作符
- 支持背压，不丢数据

优势：轻量级，可直接在业务代码中使用

劣势：
- 内部执行流程复杂，容易踩坑，不如Flink傻瓜化
- 没有watermark功能，也就意味着只支持无序数据处理
- 没有savepoint功能，虽然我们用背压解决了部分问题，但是宕机后开始会丢失缓存队列和消费者线程池里的数据，补救措施是添加Java Hook功能
- 只支持单机，意味着你的缓存队列不能设置无限大，要考虑线程池的大小，且没有flink globalWindow等功能
- 需考虑对上游数据源的影响，Flink的上游一般是mq，数据量大时可自动堆积，如果本文的方案上游是http、rpc调用，产生的阻塞影响就不能忽略。补偿方案是每次提交数据都使用异步方法，如果失败则提交到mq中缓冲并消费该mq无限重试。

### 四、附录
Reactor官方文档：https://projectreactor.io/docs/core/release/reference/
Flink文档：https://ci.apache.org/projects/flink/flink-docs-stable/
Reactive操作符：http://reactivex.io/documentation/operators.html

---

作者：**木小丰**

博客：https://lesofn.com

**公共号：Java研发**

![](https://blogpic.chekuspace.com/二维码小_1607785087313.jpg)