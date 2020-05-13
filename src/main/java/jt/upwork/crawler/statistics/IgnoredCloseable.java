package jt.upwork.crawler.statistics;

/**
 * Closeable to ignore the Exception in method {@link IgnoredCloseable#close()}.
 */
public interface IgnoredCloseable extends AutoCloseable {
    
    @Override
    void close();
}
