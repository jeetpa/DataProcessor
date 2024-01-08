package com.example.dataprocessor;

import com.example.dataprocessor.exceptions.DataProcessingException;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CsvDataProcessor extends Thread implements DataProcessor{
    private List<File> files;
    private List<String> selectedMetrics;
    private List<String> selectedDimensions;


    public CsvDataProcessor(List<File> files, List<String> selectedDimensions, List<String> selectedMetrics){
        this.files = files;
        this.selectedMetrics = selectedMetrics;
        this.selectedDimensions = selectedDimensions;
    }

    @Override
    public Map<String, Map<String, Double>> process() throws DataProcessingException {
        ExecutorService executorService = Executors.newFixedThreadPool(files.size());

        try
        {
            CountDownLatch latch = new CountDownLatch(files.size());

            List<Worker> workers = new ArrayList<>();
            for (File file: files)
            {
                Worker worker = new Worker(file, latch);
                workers.add(worker);
                executorService.execute(worker);
            }
            latch.await();

            Map<String, Map<String, Double>> result = new HashMap<>();
            for (Worker worker: workers)
            {
                if (worker.getException() != null) {
                    throw new DataProcessingException(worker.getException());
                }
                Map<String, Map<String, Double>> workerResult = worker.getData();
                for (Map.Entry<String, Map<String,Double>> entry : workerResult.entrySet()) {
                    String key = entry.getKey();
                    Map<String,Double> value = entry.getValue();
                    if(!result.containsKey(key)){
                        result.put(key,value);
                    }
                    else{
                        Map<String, Double> existingMap = result.get(key);

                        for (Map.Entry<String, Double> innerEntry: existingMap.entrySet())
                        {
                            String metric = innerEntry.getKey();
                            Double existingValue = innerEntry.getValue();
                            Double valueToBeAdded = value.get(metric);
                            existingMap.put(metric, existingValue + valueToBeAdded);
                        }
                    }
                }
            }

            return result;

        } catch (InterruptedException e)
        {
            throw new DataProcessingException(e);
        } finally {
            executorService.shutdown();
        }

    }

    private class Worker implements Runnable {
        private final File file;
        private final Map<String, Map<String, Double>> data = new HashMap<>();
        private final CountDownLatch latch;
        private Exception e;

        public Worker(File file, CountDownLatch latch) {
            this.file = file;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                data.putAll(filereader(file));
            } catch (Exception e) {
                this.e = e;
            } finally {
                latch.countDown();
            }
        }

        public Map<String, Map<String, Double>> getData() {
            return data;
        }

        public Exception getException() {
            return e;
        }

        public Map<String, Map<String, Double>> filereader(File file) throws FileNotFoundException {
            Map<String,Map<String,Double>> result = new HashMap<>();
            BufferedReader reader = null;
            String line = "";
            try {
                reader = new BufferedReader(new FileReader(file));
                Boolean firstline = true;

                Map<String, Integer> headerIndexMap = new HashMap<>();
                while ((line = reader.readLine()) != null) {
                    String[] row = line.split(",");
                    if (firstline) {
                        for (int i = 0; i < row.length; i++) {
                            headerIndexMap.put(row[i], i);
                        }
                        firstline = false;
                        continue;
                    }
                    // metrics: ["clicks", "imp", "rev"]
                    // dimensions: ["report_date", "siteid", "country"]
                    // stringArray: ["2023-09-04","1289","ID","678","5048","12.18"]
                    // headerIndexMap: {"report_date": 0, "siteid": 1, "country": 2, "clicks": 3, "imp": 4, "rev": 5}
                    String resultkey = getKey(row, headerIndexMap);
                    Map<String, Double> metricMap = getMetricMap(row, headerIndexMap);
                    if (result.containsKey(resultkey)) {
                        result.put(resultkey, mergeMetrics(result.get(resultkey), metricMap));
                    } else {
                        result.put(resultkey, metricMap);
                    }
                }
                reader.close();

            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return result;

        }



        private String getKey(String[] row, Map<String, Integer> headerIndexMap)
        {
            StringBuilder sb = new StringBuilder();
            for(int i=0;i<selectedDimensions.size();i++){
                Integer index = headerIndexMap.get(selectedDimensions.get(i));
                sb.append(row[index]);
                if(i != selectedDimensions.size()-1){
                    sb.append(":");

                }
            }
            return sb.toString();
        }
        private Map<String, Double> getMetricMap(String[] row, Map<String, Integer> headerIndexMap)
        {
            Map<String,Double> selectedMetricMap = new HashMap<>();
            for(String selectedMetric : selectedMetrics){
                Integer index = headerIndexMap.get(selectedMetric);
                Double value = Double.valueOf(row[index]);
                selectedMetricMap.put(selectedMetric,value);
            }
            return  selectedMetricMap;
        }

        private Map<String, Double> mergeMetrics(Map<String, Double> existingMap, Map<String, Double> newMap)
        {
            for (Map.Entry<String, Double> entry: newMap.entrySet())
            {
                String metric = entry.getKey();
                Double value = entry.getValue();

                Double existingValue = existingMap.get(metric);

                existingMap.put(metric, existingValue + value);
            }

            return existingMap;
        }
    }

}

