import com.example.dataprocessor.CsvDataProcessor;
import com.example.dataprocessor.DataProcessor;
import com.example.dataprocessor.exceptions.DataProcessingException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
public class Driver {
    public static void main(String[] args) throws  DataProcessingException {
        List<File> files = new ArrayList<>();

        BufferedReader reader = null;
        try {
            System.out.println(args[0]);
            reader = new BufferedReader(new FileReader(args[0]));
            String[] fileNames = reader.readLine().split(",");
            String[] dimensions = reader.readLine().split(",");
            String[] metrics = reader.readLine().split(",");
            for(String fileName : fileNames){
                files.add(new File(fileName));
            }

            DataProcessor processor = new CsvDataProcessor(files, Arrays.asList(dimensions),Arrays.asList(metrics));
            System.out.println(processor.process());

        }catch (IOException e){
            e.printStackTrace();
        }
        finally {
            try {
                reader.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

}
