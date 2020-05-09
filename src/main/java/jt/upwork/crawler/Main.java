package jt.upwork.crawler;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.LogManager;

/**
 * Main class
 *
 * @author jamestravol
 */
public class Main {

    public static void main(String[] args) throws IOException, InvocationTargetException, IllegalAccessException {
        LogManager.getLogManager().readConfiguration(ProcessingScript.class.getResourceAsStream("/logger.properties"));
        new ProcessingScript().execute();
    }

}
