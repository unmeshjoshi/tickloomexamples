package com.example.perf;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

//Assignment1: Find out throughput of your disc.
public class DiskWritePerformanceTest {

    private static final String FILE_NAME = "testfile.bin";
    private static final int WRITE_SIZE = 1024; // Size of each write in bytes
    // (1 KB)
    private static final int DURATION_IN_SECONDS = 20; // Duration of the test in seconds

    public static void main(String[] args) throws IOException {
        byte[] data = createData(WRITE_SIZE);
        File perfFile = createFile(FILE_NAME);

        System.out.println("Writing data to = " + perfFile);

        PerformanceMetrics metrics = performWriteTest(perfFile, data, DURATION_IN_SECONDS);

        printMetrics(metrics);
    }

    private static byte[] createData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = 'A';
        }
        return data;
    }

    private static File createFile(String fileName) {
        return new File(TestUtils.tempDir("perf"), fileName);
    }

    private static PerformanceMetrics performWriteTest(File file, byte[] data
            , int durationInSeconds) throws IOException {
        Instant startTime = Instant.now();
        Instant endTime = startTime.plus(Duration.ofSeconds(durationInSeconds));


        long numberOfWrites = writeUntil(endTime, file, data);

        Instant actualEndTime = Instant.now();
        Duration duration = Duration.between(startTime, actualEndTime);

        return new PerformanceMetrics(numberOfWrites, duration.getSeconds(), WRITE_SIZE);
    }

    private static long writeUntil(Instant endTime, File file, byte[] data) throws IOException {
        long numberOfWrites = 0;
        FileOutputStream os =
                     new FileOutputStream(file);
        while (Instant.now().isBefore(endTime)) {
            os.write(data);
            os.flush();
            //crash at this point..
            syncToPhysicalMedia(os);

            numberOfWrites++;
        }
        return numberOfWrites;
    }

    private static void syncToPhysicalMedia(FileOutputStream os) throws IOException {
        os.getFD().sync(); //Why??
    }

    private static void printMetrics(PerformanceMetrics metrics) {
        double writesPerSecond = metrics.getNumberOfWrites() / metrics.getSeconds();
        double mbWritten = (metrics.getNumberOfWrites() * metrics.getWriteSize()) / (1024.0 * 1024.0);
        double mbPerSecond = mbWritten / metrics.getSeconds();

        System.out.println("Total writes: " + metrics.getNumberOfWrites());
        System.out.println("Total time: " + metrics.getSeconds() + " seconds");
        System.out.println("Writes per second: " + writesPerSecond);
        System.out.println("MB written: " + mbWritten);
        System.out.println("MB per second: " + mbPerSecond);
    }

    private static class PerformanceMetrics {
        private final long numberOfWrites;
        private final double seconds;
        private final int writeSize;

        public PerformanceMetrics(long numberOfWrites, double seconds, int writeSize) {
            this.numberOfWrites = numberOfWrites;
            this.seconds = seconds;
            this.writeSize = writeSize;
        }

        public long getNumberOfWrites() {
            return numberOfWrites;
        }

        public double getSeconds() {
            return seconds;
        }

        public int getWriteSize() {
            return writeSize;
        }
    }
}
