package edu.berkeley.thebes.common.config;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IOReporter {
    
    private static final int MIN_TIME_GRANULARITY = 2000;
    private static final double EMA_ALPHA = .8;
    
    private long lastRxBytes, lastTxBytes;
    private long lastRun = 0;
    
    private double rxRateKbs;
    private double txRateKbs;
    
    final Histogram rxRateHistogram = Metrics.newHistogram(IOReporter.class, "net-rx-rate-KBs");
    final Histogram txRateHistogram = Metrics.newHistogram(IOReporter.class, "net-tx-rate-KBs");

    public IOReporter() {
        
    }
    
    public void computeNetworkRates() {
        long curTime = System.currentTimeMillis();
        long rxBytes = 0, txBytes = 0;

        try {
            Process proc = Runtime.getRuntime().exec("netstat -i eth0 -t -u -e");
            proc.waitFor();
            
            BufferedReader bufIn = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            while (bufIn.ready()) {
                String line = bufIn.readLine();
                if (line.contains("RX bytes")) {
                    Pattern pattern = Pattern.compile("RX bytes:([0-9]+) .* TX bytes:([0-9]+)");
                    Matcher match = pattern.matcher(line);
                    if (match.find()) {
                        rxBytes = Long.parseLong(match.group(1));
                        txBytes = Long.parseLong(match.group(2));
                        break;
                    }
                }
            }
            bufIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
        if (txBytes == 0 || rxBytes == 0) {
            return;
        }
        
        if (lastRun != 0) {
            long timeDiff = curTime - lastRun;
            double instantRxKbs = (rxBytes-lastRxBytes) / 1000d / timeDiff;
            double instantTxKbs = (txBytes-lastTxBytes) / 1000d / timeDiff;
            
            rxRateKbs = EMA_ALPHA * instantRxKbs + (1-EMA_ALPHA) * rxRateKbs;
            rxRateHistogram.update((long) rxRateKbs);
            txRateKbs = EMA_ALPHA * instantTxKbs + (1-EMA_ALPHA) * txRateKbs;
            txRateHistogram.update((long) txRateKbs);
        }
        
        lastRxBytes = rxBytes;
        lastTxBytes = txBytes;
        lastRun = curTime;
    }
    
    public void start() {
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(MIN_TIME_GRANULARITY);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    
                    computeNetworkRates();
                }
            }
        }.start();
    }
    
    public static void main(String [] args) {
        IOReporter i = new IOReporter();
        i.start();
    }
}
