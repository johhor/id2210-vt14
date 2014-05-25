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
    
    public T get99thPercentile(){
        Collections.sort(statisticsList, comp);
        //95% of the list size
        double listSize = statisticsList.size();
        int prcIndex = (int)Math.floor(listSize*0.99);
        if(prcIndex == statisticsList.size())
            prcIndex = statisticsList.size() - 1;
        if(prcIndex < 0)
            prcIndex = 0;
        return statisticsList.get(prcIndex);
    }
}