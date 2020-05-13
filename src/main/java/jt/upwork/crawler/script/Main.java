package jt.upwork.crawler.script;

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
        //config the logger
        LogManager.getLogManager().readConfiguration(ProcessingScript.class.getResourceAsStream("/logger.properties"));
        // start script
        new ProcessingScript().execute();
    }

}
