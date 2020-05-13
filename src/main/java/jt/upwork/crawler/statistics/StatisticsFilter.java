package jt.upwork.crawler.statistics;

import jt.upwork.crawler.statistics.RuntimeStatistics;

import java.util.logging.Filter;
import java.util.logging.LogRecord;

public class StatisticsFilter implements Filter {
    @Override
    public boolean isLoggable(LogRecord record) {
        return record.getSourceClassName().equals(RuntimeStatistics.class.getName());
    }
}
