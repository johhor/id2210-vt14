
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
    private int nodeName;
    public RunTimeStatistics(int nodeName){
        Comparator<Long> comp = createComparator();
        allocationTimes = new StatisticsSet<Long>(comp);
        searchTimes = new StatisticsSet<Long>(comp);
        this.nodeName = nodeName;
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
        
        try {
            PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter("temp"+".tst",true)));
            writer.print(time+",");//"["+(++t)+"]"+time+", ");
            writer.flush();
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(RunTimeStatistics.class.getName()).log(Level.SEVERE, null, ex);
        }  
    }
    public void closeWriter(){
            
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
    public ArrayList<Long> get99thPercentileAllocationTimes(){
         return allocationTimes.get99thPercentile();
    }
    public ArrayList<Long> get99thPercentileSearchTimes(){
        return searchTimes.get99thPercentile();
    }
}
