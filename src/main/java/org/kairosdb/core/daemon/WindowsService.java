package org.kairosdb.core.daemon;

import ch.qos.logback.classic.Logger;
import org.kairosdb.core.Main;
import org.slf4j.LoggerFactory;

import java.io.File;

public class WindowsService {
    public static final Logger logger = (Logger) LoggerFactory.getLogger(WindowsService.class);

    private static String[] startKairosDbArgs;
    private static Main mainKairosDbRunner;
    private static final String keyProcessFileDir =  "processFileDir=";
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            logger.info("NOTE: args.length == 0");
            System.exit(0);
        }

        if (args[0].toLowerCase().trim().equals("stop"))
        {
            if(args.length < 2){
                logger.info("NOTE: args.length < 2, stop params not defined");
                return;
            }
            if(!args[1].contains(keyProcessFileDir)){
                logger.info("NOTE: processFileDir parameter is not defined");
                return;
            }
            stop(args[1].replace(keyProcessFileDir, ""));
        }
        else
        {
            start(args);
        }
    }

    public static void start(String [] args) throws Exception {

        String watchDirParam = args[0];
        if(!watchDirParam.contains(keyProcessFileDir)){
            logger.error("First param to run kairosDb as Windows Service must be 'processFileDir=' containing directory path");
            System.exit(0);
        }

        String watchDir = args[0].replace(keyProcessFileDir, "");
        startKairosDbArgs = new String[args.length - 1];
        for (int i = 0; i < startKairosDbArgs.length; i++)
            startKairosDbArgs[i] = args[i + 1];

        mainKairosDbRunner = Main.GetSingletonInstance(startKairosDbArgs);
        Thread thread = new Thread(() -> {
           try{
               new WatchDir(watchDir, mainKairosDbRunner, false).processEvents();
               logger.info("WatchDir process has been initiated");
           } catch (Throwable e){
               logger.error(e.getMessage());
               logger.error("Watch directory process was interrupted");
           }
        });
        thread.start();
        mainKairosDbRunner.startKairosDb();
    }

    public static void stop(String processFileDir) throws Exception {

        logger.info("------------------------------------------");
        logger.info("     Trying to stop KairosDB service");
        logger.info("------------------------------------------");

        File file = WatchDir.GetProcessFile(processFileDir);
        if(file.exists()){
            file.delete();
        }
		
        Thread.sleep(5000);
		
        logger.info("------------------------------------------");
        logger.info("     KairosDB service has been stopped");
        logger.info("------------------------------------------");
    }
}
