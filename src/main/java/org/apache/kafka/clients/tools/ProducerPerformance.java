/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.tools;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.json.JSONException;
import org.json.JSONStringer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

public class ProducerPerformance {

    private static Logger LOG = Logger.getLogger(ProducerPerformance.class);

    private static final String TRACE_KEY = "Trace";
    private static final String DEPTH_KEY = "Depth";
    private static final String TEMPERATURE_KEY = "Temp";

    private static final long NS_PER_MS = 1000000L;
    private static final long NS_PER_SEC = 1000 * NS_PER_MS;
    private static final long MIN_SLEEP_NS = 2 * NS_PER_MS;

    private static final String SKO_KAFKA_CLUSTER_ADDRESS = "scregionsko1502.cloud.hortonworks.com:6667" ;
    private static final String SKO_RAW_DTS_DATA_FILE = "/Users/phargis/Downloads/1000traces.tsv" ;

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("USAGE: java " + ProducerPerformance.class.getName() +
                               " topic_name max_records target_records_sec [prop_name=prop_value]*");
            System.exit(1);
        }

        /* parse args */
        String topicName = args[0];
        long numRecords = Long.parseLong(args[1]);
        int throughput_target = Integer.parseInt(args[2]);

        Properties props = new Properties();
        for (int i = 3; i < args.length; i++) {
            String[] pieces = args[i].split("=");
            if (pieces.length != 2)
                throw new IllegalArgumentException("Invalid property: " + args[i]);
            props.put(pieces[0], pieces[1]);
        }
        // Configure Kafka Cluster address
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SKO_KAFKA_CLUSTER_ADDRESS);
        KafkaProducer producer = new KafkaProducer(props);

        BufferedReader br = openDataFile(SKO_RAW_DTS_DATA_FILE);

        long sleepTime = NS_PER_SEC / throughput_target;
        long sleepDeficitNs = 0;
        Stats stats = new Stats(numRecords, 5000);
        // Read data from file, convert to JSON, and send message to Kafka
        for (int ii = 0; ii < numRecords; ii++) {
            String line = br.readLine();
            if (line == null || line.length() == 0) {
                break;
            }

            String jsonMessage = convertRawDataToJson(line);

            /* Load data into payload */
            byte[] payload = Bytes.toBytes(jsonMessage);
            ProducerRecord record = new ProducerRecord(topicName, payload);

            long sendStart = System.currentTimeMillis();
            Callback cb = stats.nextCompletion(sendStart, payload.length, stats);
            producer.send(record, cb);

            /*
             * Maybe sleep a little to control throughput. Sleep time can be a bit inaccurate for times < 1 ms so
             * instead of sleeping each time instead wait until a minimum sleep time accumulates (the "sleep deficit")
             * and then make up the whole deficit in one longer sleep.
             */
            if (throughput_target > 0) {
                sleepDeficitNs = periodicSleep(sleepTime, sleepDeficitNs);
            }
        }

        /* print final results */
        producer.close();
        stats.printTotal();
    }

    private static long periodicSleep(long sleepTime, long sleepDeficitNs) throws InterruptedException {
        sleepDeficitNs += sleepTime;
        if (sleepDeficitNs >= MIN_SLEEP_NS) {
            long sleepMs = sleepDeficitNs / 1000000;
            long sleepNs = sleepDeficitNs - sleepMs * 1000000;
            Thread.sleep(sleepMs, (int) sleepNs);
            sleepDeficitNs = 0;
        }
        return sleepDeficitNs;
    }

    private static BufferedReader openDataFile (String name) {
        BufferedReader inputReader = null;
        try {
            inputReader = new BufferedReader(new FileReader(name));
        } catch (FileNotFoundException ex) {
            LOG.error("Cannot open file: "+name);
        }
        return inputReader;
    }

    private static String convertRawDataToJson(String line) {
        String[] tokens = new String[3];

        StringTokenizer tokenizer = new StringTokenizer(line, "\t");
        int ii = 0;
        while (tokenizer.hasMoreTokens()) {
            tokens[ii++] = tokenizer.nextToken();
        }

        DateTime traceTime = DateTime.parse(tokens[0]);
        Double depthValue = Double.parseDouble(tokens[1]);
        Double tempValue = Double.parseDouble(tokens[2]);

        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            stringer.key(TRACE_KEY).value(traceTime);
            stringer.key(DEPTH_KEY).value(depthValue);
            stringer.key(TEMPERATURE_KEY).value(tempValue);
            stringer.endObject();
        } catch (JSONException ex) {

        }

        return stringer.toString();
    }


    private static class Stats {
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        public Stats(long numRecords, int reportingInterval) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.index = 0;
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.reportingInterval = reportingInterval;
        }

        public void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            /* maybe report the recent perf */
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public Callback nextCompletion(long start, int bytes, Stats stats) {
            Callback cb = new PerfCallback(this.iteration, start, bytes, stats);
            this.iteration++;
            return cb;
        }

        public void printWindow() {
            long ellapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) ellapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) ellapsed / (1024.0 * 1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.2f MB/sec), %.1f ms avg latency, %.1f max latency.\n",
                              windowCount,
                              recsPerSec,
                              mbPerSec,
                              windowTotalLatency / (double) windowCount,
                              (double) windowMaxLatency);
        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
        }

        public void printTotal() {
            long ellapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) ellapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) ellapsed / (1024.0 * 1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            System.out.printf("%d records sent, %f records/sec (%.2f MB/sec), %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.\n",
                              count,
                              recsPerSec,
                              mbPerSec,
                              totalLatency / (double) count,
                              (double) maxLatency,
                              percs[0],
                              percs[1],
                              percs[2],
                              percs[3]);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }
    }

    private static final class PerfCallback implements Callback {
        private final long start;
        private final int iteration;
        private final int bytes;
        private final Stats stats;

        public PerfCallback(int iter, long start, int bytes, Stats stats) {
            this.start = start;
            this.stats = stats;
            this.iteration = iter;
            this.bytes = bytes;
        }

        public void onCompletion(RecordMetadata metadata, Exception exception) {
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            this.stats.record(iteration, latency, bytes, now);
            if (exception != null)
                exception.printStackTrace();
        }
    }

}
