/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ounl.tridentreadyperformancestatistics;

import com.mongodb.Block;
import java.util.HashMap;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

/**
 *
 * @author gla
 */
public class StatisticalAlgorithmsTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        runManualTest();
    }
    
    public static void runManualTest() {
        // Simulate sequential input in Apache Trident
        // The traces are provided in order of incoming, each input represents one incoming trace
        
        HashMap input = new HashMap();
        //gameID;taskID;studentID;groupID;date     ;score      ;timeToComplete
//        Game A;Task 1;Student 1;Group 1;19-5-2016;0.159110123;0.225256615;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("studentID", "Student 1");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 0.159110123);
        input.put("timeToComplete", 0.225256615);
        updateDistribution(input);
        input.clear();
        
//        Game A;Task 1;Student 2;Group 1;19-5-2016;1.193601535;1.436718613;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("studentID", "Student 2");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 1.193601535);
        input.put("timeToComplete", 1.436718613);
        updateDistribution(input);
        input.clear();
        
//        Game A;Task 1;Student 3;Group 1;19-5-2016;0.800408171;0.268165926;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("studentID", "Student 3");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 0.800408171);
        input.put("timeToComplete", 0.268165926);
        updateDistribution(input);
        input.clear();

//        Game A;Task 1;Student 4;Group 1;19-5-2016;1.146483123;1.643873708;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("studentID", "Student 4");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 1.146483123);
        input.put("timeToComplete", 1.643873708);
        updateDistribution(input);
        input.clear();
    }
    
    static void updateDistribution(HashMap input) {
        // Initially we only use score in these performance statistics, so timeToComplete is redundant here
        
        // A long is used here to simulate input from tuple
        String gameID = (String)input.get("gameID");        
        String taskID = (String)input.get("taskID");
        String playerID = (String)input.get("playerID");
        String groupID = (String)input.get("groupID");
        String date = (String)input.get("date");
        double score = (Double)input.get("score");
        double timeToComplete = (Double)input.get("timeToComplete");
        long trial = (Long)input.get("trial");
        
        // We store data in mongoDB. There is one DB for the traces and one for the statistics results
        
        // Step 1: retreive the trialnum by looking at the previous traces
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("performanceStatisticsDB");
        
        // Find all database entries for a specific task in a specific game for a specific player
        FindIterable<Document> iterable = db.getCollection("traces")
                .find(new Document("gameID", gameID)
                .append("taskID",taskID)
                .append("playerID",playerID));
        
        iterable.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                System.out.println(document);
            }
        });
        
        //New N, code guarantees N of 1 or higher
        Long oldN = trial;
        
        if (oldN <= 0) {
            
            oldN = 1L;
        } else {
            oldN++;
        }
                
        //New mean, variance, & stdDev >> based on: http://www.johndcook.com/blog/standard_deviation/
        Double oldMean = oldVect.mean;
        Double newMean;
        Double oldSD = oldVect.SD;
        Double newS;
        
        if (oldN == 1) {
            newMean = score;
            newS = 0D;
        } else {
            //New means formula suitable for big data
            //Previous code guarantees tmpN > 0
            newMean = oldMean + (input - oldMean) / oldN;
            Double oldS = ((oldSD*oldSD)*oldN)-((input-oldMean)*(input-newMean));
            newS = oldS + (input - oldMean)*(input - newMean);
        }
        
        TaskVector newVect = new TaskVector();
        newVect.mean = newMean;
        
        newVect.SD = Math.sqrt((oldN > 1) ? (newS / (oldN - 1)) : 0D);
        newVect.n = oldN;
        
        // For testing purposes
        System.out.println(String.valueOf(newVect.mean)+ " " 
            + String.valueOf(newVect.SD)+ " " + String.valueOf(newS) + " "     
            + String.valueOf(newVect.n));
        
        /*
        //New skewness & kurtosis >> based on: http://www.johndcook.com/blog/skewness_kurtosis/
        double delta, delta_n, delta_n2, term1;
        
        delta = input - newMean;
        delta_n = delta / oldVect.n;
        delta_n2 = delta_n * delta_n;
        term1 = delta * delta_n * oldVect.n;
        help3 = (term1 * delta_n2 * (oldVect.n*oldVect.n-3*oldVect.n+3))+(6*delta_n2*help1)-(4*delta_n*help2);
        help2 = (term1 * delta_n * (oldVect.n - 2)) - (3 * oldVect.n * delta_n * help1);
        help1 = term1;
        
        setSkewness(Math.sqrt((double)oldVect.n) * help2/ Math.pow(help1, 1.5));
        setKurtosis(((double)oldVect.n)*help3 / (help1*help1) - 3.0);*/
    }
}