
package common.peer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
    private StatisticsSet<Long> allocationTimes;
    private StatisticsSet<Long> searchTimes;
    
    public RunTimeStatistics(){
        Comparator<Long> comp = createComparator();
        allocationTimes = new StatisticsSet<Long>(comp);
        searchTimes = new StatisticsSet<Long>(comp);
    }
    
    public Comparator<Long> createComparator(){
        return new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                return Long.compare(o1, o2);
            }
        };
    }
    private int t = 0;
    private int j=0;
    public void addAllocationTime(long time){
        allocationTimes.addData(time);  
    }    
    public void printAllData(String fileName){
        for(Long time : getAllocationTimes() ){
            printData(fileName, time);
        }
    }
    public void printData(String fileName,long time){
        try {
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(fileName,true)));
            writer.print(time+",");
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(RunTimeStatistics.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    public Long getAllocationTimeMeanValue(){
        long sum = 0;
        for(Long l : allocationTimes.getData())
            sum += l;
        int size = allocationTimes.getData().size();
        if(size <= 0)
            return new Long(0);
        return sum/size;
    }
    public void addSearchTime(long time){
        allocationTimes.addData(time);
    }
    public ArrayList<Long> getAllocationTimes(){
        return allocationTimes.getData();
    }
    public ArrayList<Long> getSearchTimes(){
        return searchTimes.getData();
    }
    public long get99thPercentileAllocationTimes(){
        Long tmp = allocationTimes.get99thPercentile();
        if (tmp == null)
            return 0;
        return allocationTimes.get99thPercentile();
    }
    public Long get99thPercentileSearchTimes(){
        return searchTimes.get99thPercentile();
    }
}