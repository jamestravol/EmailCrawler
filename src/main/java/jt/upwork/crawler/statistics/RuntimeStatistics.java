package jt.upwork.crawler.statistics;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * THis class represents the statistics about execution stages
 */
public class RuntimeStatistics {

    private static final Logger LOGGER = Logger.getLogger(RuntimeStatistics.class.getName());
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("mm:ss");

    private final AtomicLong startMills = new AtomicLong();

    /**
     * The data requesting from the database
     */
    private final RuntimeStageStatistic dataRequest = new RuntimeStageStatistic();

    /**
     * A whole website processing
     * this includes:
     * {@link RuntimeStatistics#page}
     * {@link RuntimeStatistics#webRequest}
     * {@link RuntimeStatistics#parsing}
     * {@link RuntimeStatistics#processing}
     * {@link RuntimeStatistics#callback}
     */
    private final RuntimeStageStatistic website = new RuntimeStageStatistic();

    /**
     * A single page processing
     * {@link RuntimeStatistics#webRequest}
     * {@link RuntimeStatistics#parsing}
     * {@link RuntimeStatistics#processing}
     * {@link RuntimeStatistics#callback}
     */
    private final RuntimeStageStatistic page = new RuntimeStageStatistic();

    /**
     * Web request processing
     */
    private final RuntimeStageStatistic webRequest = new RuntimeStageStatistic();

    /**
     * Page parsing process processing
     */
    private final RuntimeStageStatistic parsing = new RuntimeStageStatistic();

    /**
     * General crawling processing processing
     */
    private final RuntimeStageStatistic processing = new RuntimeStageStatistic();

    /**
     * Processing of hte callback
     */
    private final RuntimeStageStatistic callback = new RuntimeStageStatistic();

    /**
     * Invoke when the general process begins
     */
    public void start() {
        startMills.set(System.currentTimeMillis());
    }

    public RuntimeStageStatistic getDataRequest() {
        return dataRequest;
    }

    public RuntimeStageStatistic getWebsite() {
        return website;
    }

    public RuntimeStageStatistic getPage() {
        return page;
    }

    public RuntimeStageStatistic getWebRequest() {
        return webRequest;
    }

    public RuntimeStageStatistic getParsing() {
        return parsing;
    }

    public RuntimeStageStatistic getProcessing() {
        return processing;
    }

    public RuntimeStageStatistic getCallback() {
        return callback;
    }

    public void log(ForkJoinPool forkJoinPool) {
        final String message = String.format(
                "\rTime: %s. Threads active: %d, run: %d. Queue: %d. Tasks: %d. Data: %d-%d ms. Websites: %d/%d-%d ms. Pages: %d/%d-%d ms. Requests: %d-%d ms. Parsings: %d-%d ms. Processings: %d-%d ms. Callbacks: %d/%d-%d ms.",
                TIME_FORMATTER.format(Instant.ofEpochMilli(System.currentTimeMillis() - startMills.get()).atZone(ZoneId.systemDefault()).toLocalDateTime()),
                forkJoinPool.getActiveThreadCount(), forkJoinPool.getRunningThreadCount(), forkJoinPool.getQueuedSubmissionCount(), forkJoinPool.getQueuedTaskCount(),
                dataRequest.getInProgress(), dataRequest.getAverageExecutionTime(),
                website.getInProgress(), website.getTriesCount(), website.getAverageExecutionTime(),
                page.getInProgress(), page.getTriesCount(), page.getAverageExecutionTime(),
                webRequest.getInProgress(), webRequest.getAverageExecutionTime(),
                parsing.getInProgress(), parsing.getAverageExecutionTime(),
                processing.getInProgress(), processing.getAverageExecutionTime(),
                callback.getInProgress(), callback.getTriesCount(), callback.getAverageExecutionTime());
        System.out.print(message);
    }
}
