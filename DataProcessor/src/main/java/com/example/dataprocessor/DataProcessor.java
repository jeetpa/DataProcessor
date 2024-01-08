package com.example.dataprocessor;

import com.example.dataprocessor.exceptions.DataProcessingException;

import java.io.FileNotFoundException;
import java.util.Map;

public interface DataProcessor {

    public Map<String,Map<String,Double>> process() throws DataProcessingException;
}
