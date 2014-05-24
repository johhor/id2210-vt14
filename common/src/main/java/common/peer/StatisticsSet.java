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
        //System.out.println("Startindex is:"+startIndex + " and size is: "+ statisticsList.size());
        return statisticsList.get(prcIndex);
    }
    
      /* Testing function for the percentile*/    
//    public static void main(String[] args){
//        Comparator<Double> comp = new Comparator<Double>() {
//
//            @Override
//            public int compare(Double o1, Double o2) {
//                return Double.compare(o1, o2);
//            }
//        };
//        StatisticsSet<Double> set = new StatisticsSet<Double>(comp);
//        
//        for(double i = 0; i < 1000; i += 0.1){
//            set.addData(i);
//        }
////        set.addData(0.1);
////        set.addData(0.2);
//        
//        ArrayList<Double> percentil = set.get99thPercentile();
//        int j = 0;
//        for(int i = 0; i < percentil.size(); i++){
//            double d = percentil.get(i);
//            
//            System.out.print(d); 
//            if (percentil.size()-1 > i)
//                System.out.print( " , ");
//            j++;
//            if(j > 10){
//                System.out.println();
//                j=0;
//            }
//        }
//        System.out.print("\n\n");
//    }
}