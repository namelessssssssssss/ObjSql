package com.objsql.client.message;

import com.objsql.common.util.common.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * 请求队列
 */
@Slf4j
public class MissionQueue {

    /**
     * 提交的任务
     */
    private static final LinkedBlockingDeque<ClientRequest> MISSIONS = new LinkedBlockingDeque<>(512);
    /**
     * 暂存的请求对象
     */
    private static final Map<Integer, ClientRequest> REQUESTS = new ConcurrentHashMap<>();
    /**
     * 处理完成的任务结果
     */
    private static final Map<Integer, HandledServerResponse> RESULTS = new ConcurrentHashMap<>();
    /**
     * 被阻塞线程
     */
    private static final Map<Integer, CountDownLatch> BLOCKED = new ConcurrentHashMap<>();
    /**
     * 响应后的回调
     */
    private static final Map<Integer, Consumer<HandledServerResponse>> AFTER_RESPONSE = new ConcurrentHashMap<>();

    private static final ExecutorService WORKERS = new ThreadPoolExecutor(
            1, 10, 10, TimeUnit.SECONDS, new ArrayBlockingQueue<>(5)
    );

    private static final AtomicInteger sequenceId = new AtomicInteger();

    /**
     * 提交请求，不进行阻塞
     */
    public static void submitAsync(ClientRequest request, Consumer<HandledServerResponse> afterResponse) {
        try {
            request.setSequenceId(sequenceId.getAndIncrement());
            MISSIONS.add(request);
            REQUESTS.put(request.getSequenceId(), request);
            if (afterResponse != null) {
                AFTER_RESPONSE.put(request.getSequenceId(), afterResponse);
            }
        } catch (Exception e) {
            log.error(ExceptionUtil.getStackTrace(e));
        }
    }

    public static void submitAsync(ClientRequest request) {
        submitAsync(request, null);
    }

    /**
     * 提交请求，阻塞到服务器响应
     */
    public static HandledServerResponse submit(ClientRequest request) {
        try {
            request.setSequenceId(sequenceId.getAndIncrement());
            CountDownLatch latch = new CountDownLatch(1);
            BLOCKED.put(request.getSequenceId(),latch);
            MISSIONS.add(request);
            REQUESTS.put(request.getSequenceId(), request);
            latch.await();
            return RESULTS.remove(request.getSequenceId());
        } catch (Exception e) {
            log.error(ExceptionUtil.getStackTrace(e));
        }
        return null;
    }

    /**
     * 完成并响应一条请求
     */
    public static void finish(int sequenceId, HandledServerResponse response){
        CountDownLatch latch = BLOCKED.remove(sequenceId);
        REQUESTS.remove(sequenceId);
        if (latch != null) {
            if (response.getErrorMessage() != null) {
                log.error("请求服务器失败，原因：\n" + response.getErrorMessage());
            } else {
                RESULTS.put(sequenceId, response);
            }
            latch.countDown();
        }
        Consumer<HandledServerResponse> callback = AFTER_RESPONSE.remove(sequenceId);
        if (callback != null ) {
            if(response.getErrorMessage() == null) {
                WORKERS.submit(() -> callback.accept(response));
            }
            else {
                log.warn("请求服务器失败，原因：\n" + response.getErrorMessage());
            }
        }
    }

    public static ClientRequest getRequest(int sequenceId) {
        return REQUESTS.get(sequenceId);
    }

    public static ClientRequest getRequestAndRemove(int sequenceId) {
        return REQUESTS.remove(sequenceId);
    }

    /**
     * 获取一个消息，若消息列表为空则阻塞
     */
    public static ClientRequest getFirst() throws InterruptedException {
        return MISSIONS.takeFirst();
    }

}
