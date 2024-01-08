import com.example.dataprocessor.CsvDataProcessor;
import com.example.dataprocessor.DataProcessor;
import com.example.dataprocessor.exceptions.DataProcessingException;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Driver {
    public static void main(String[] args) throws FileNotFoundException, InterruptedException, DataProcessingException {
        List<File> files = new ArrayList<>();
        files.add(new File("C:\\Users\\ASUS\\Downloads\\MOCK_DATA (1).csv"));
        files.add(new File("C:\\Users\\ASUS\\Desktop\\MOCK_DATA (1).csv"));
        DataProcessor processor = new CsvDataProcessor(files, Arrays.asList("report_date", "siteid"), Arrays.asList("imp", "rev"));
        System.out.println(processor.process());
    }
}
