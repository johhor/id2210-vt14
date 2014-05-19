package common.peer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class StatisticsSet<T> {
    private final ArrayList<T> statisticsList;
    private final Comparator<T> comp;
    
    public StatisticsSet(Comparator<T> c){
        statisticsList = new ArrayList<T>();
        comp = c;
    }
    
    public void addData(T data){
        statisticsList.add(data);
    }
    
    public ArrayList<T> getData(){
        //Sort in ascendng order
        Collections.sort(statisticsList, comp);
        return statisticsList;
    }
    
    public ArrayList<T> get99thPercentile(){
        //95% of the list size
        int startIndex = statisticsList.size() - (statisticsList.size()/100);
        return new ArrayList<T>(statisticsList.subList(startIndex, statisticsList.size()));
    }
}