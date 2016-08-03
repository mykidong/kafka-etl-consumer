package kafka.etl;

/**
 * Created by mykidong on 2016-08-03.
 */
public class ShutdownHookTestMain {

    public static void main(String[] args)
    {
        ShutdownHookTestMain main = new ShutdownHookTestMain();

        final Thread mainThread = Thread.currentThread();

        // Registering a shutdown hook so we can exit cleanly
        Runtime.getRuntime().addShutdownHook(new KafkaETLParquetConsumer.ShutdownHookThread(main, mainThread));
    }
    private static class ShutdownHookThread extends Thread
    {
        private ShutdownHookTestMain main;
        private Thread mainThread;

        public ShutdownHookThread(ShutdownHookTestMain kafkaETLParquetConsumer, Thread mainThread)
        {
            this.main = main;
            this.mainThread = mainThread;
        }

        public void run() {
            System.out.println("Starting exit...");
            // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
            kafkaETLParquetConsumer.consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void run()
    {
        while(true)
        {

        }
    }

    public void wakeup()
    {
        System.out.println();
    }
}
