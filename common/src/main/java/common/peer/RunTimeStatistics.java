
package common.peer;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An example class of implementation of statistics into the project
 * Implementation of the Statistics set class into one class to make it 
 * more manageable in classes using statistics
 **/
public class RunTimeStatistics {
    private StatisticsSet<Double> allocationTimes;
    private StatisticsSet<Double> searchTimes;
    private PrintWriter writer;
    
    public RunTimeStatistics(int nodeName){
        Comparator<Double> comp = createComparator();
        allocationTimes = new StatisticsSet<Double>(comp);
        searchTimes = new StatisticsSet<Double>(comp);
        try {
            writer = new PrintWriter(new BufferedWriter(new FileWriter("testStat"+nodeName+".tst",true)));
        } catch (IOException ex) {
            Logger.getLogger(RunTimeStatistics.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public Comparator<Double> createComparator(){
        return new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return Double.compare(o1, o2);
            }
        };
    }
    public void addAllocationTime(Double time){
        allocationTimes.addData(time);
        int j=0;
            for(double d : get99thPercentileAllocationTimes()){
                writer.print(d+", ");
                if (j++ > 10)
                    writer.println("");
            }
    }
    public void closeWriter(){
            writer.flush();
            writer.close();
    }
    public void addSearchTime(Double time){
        allocationTimes.addData(time);
    }
    public ArrayList<Double> getAllocationTimes(){
        return allocationTimes.getData();
    }
    public ArrayList<Double> getSearchTimes(){
        return searchTimes.getData();
    }
    public ArrayList<Double> get99thPercentileAllocationTimes(){
         return allocationTimes.get99thPercentile();
    }
    public ArrayList<Double> get99thPercentileSearchTimes(){
        return searchTimes.get99thPercentile();
    }
}
