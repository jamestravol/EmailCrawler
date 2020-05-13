package jt.upwork.crawler.statistics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Statistics class for a distinct process step
 */
public class RuntimeStageStatistic {

    /**
     * Counter that shows how much steps are in process
     */
    private final AtomicInteger inProgress = new AtomicInteger();

    /**
     * Counter that shows how much steps tried to start
     */
    private final AtomicInteger triesCount = new AtomicInteger();

    /**
     * Counter that shows how much steps are completed successfully
     */
    private final AtomicInteger successCount = new AtomicInteger();

    /**
     * This filed represents total time of all successfully finished steps
     */
    private final AtomicLong totalExecutionMills = new AtomicLong();

    /**
     * Invoke when the step starts
     *
     * @return closable to use with try-with-resources statements
     */
    public IgnoredCloseable start() {
        inProgress.incrementAndGet();
        triesCount.incrementAndGet();
        return this::stop;
    }

    /**
     * Invoke when the step is over successfully
     *
     * @param mills the execution time in mills
     */
    public void success(long mills) {
        successCount.incrementAndGet();
        totalExecutionMills.addAndGet(mills);
    }

    /**
     * Invoke when the process finished (successfully of not)
     * if try-with-resources used this method called automatically
     */
    public void stop() {
        inProgress.decrementAndGet();
    }

    /**
     * Count the average execution time for the step.
     * It could be overestimated sometime because of the multithread racing condition !!!
     *
     * @return estimation in mills
     */
    public long getAverageExecutionTime() {
        final int sucessCount = successCount.get();
        if (sucessCount != 0) {
            return totalExecutionMills.get() / sucessCount;
        } else {
            return 0L;
        }
    }

    public int getInProgress() {
        return inProgress.get();
    }

    public int getTriesCount() {
        return triesCount.get();
    }

    public int getSuccessCount() {
        return successCount.get();
    }

    public long getTotalExecutionMills() {
        return totalExecutionMills.get();
    }


}
