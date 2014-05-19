
package common.peer;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * An example class of implementation of statistics into the project
 * Implementation of the Statistics set class into one class to make it 
 * more manageable in classes using statistics
 **/
public class ProjectStatistics {
    private StatisticsSet<Double> allocationTimes;
    private StatisticsSet<Double> searchTimes;
    
    public ProjectStatistics(){
        Comparator<Double> comp = createComparator();
        allocationTimes = new StatisticsSet<Double>(comp);
        searchTimes = new StatisticsSet<Double>(comp);
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
