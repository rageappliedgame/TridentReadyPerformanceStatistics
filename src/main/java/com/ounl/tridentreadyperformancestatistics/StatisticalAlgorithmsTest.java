/**
 * Copyright (C) 2016 Open University of the Netherlands (http://www.ou.nl/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        input.put("playerID", "Student 1");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 0.159110123);
        input.put("timeToComplete", 0.225256615);
        updateDistribution(input);
        input.clear();
        
//        Game A;Task 1;Student 2;Group 1;19-5-2016;1.193601535;1.436718613;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("playerID", "Student 2");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 1.193601535);
        input.put("timeToComplete", 1.436718613);
        updateDistribution(input);
        input.clear();
        
//        Game A;Task 1;Student 3;Group 1;19-5-2016;0.800408171;0.268165926;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("playerID", "Student 3");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 0.800408171);
        input.put("timeToComplete", 0.268165926);
        updateDistribution(input);
        input.clear();

//        Game A;Task 1;Student 4;Group 1;19-5-2016;1.146483123;1.643873708;
        input.put("gameID", "Game A");
        input.put("taskID", "Task 1");
        input.put("playerID", "Student 4");
        input.put("groupID", "Group 1");
        input.put("date", "19-5-2016");
        input.put("score", 1.146483123);
        input.put("timeToComplete", 1.643873708);
        updateDistribution(input);
        input.clear();
    }
    
    static void updateDistribution(HashMap input) {
        // Initially we only use score in these performance statistics, so timeToComplete is redundant here.
        // Once score is functioning properly we will extend with timeToComplete.
        // Furthermore, the initial analysis only allows analysis of player vs all other players.
        // Once this comparison is functioning properly we will extend with player vs group, etc.
        // Finally, initialy we ignore the gameID and assume that each taskID is unique across all games.
        // Once complete, we will extend. Etc.
        
        
        // A HashMap is used to simpulate input by a tuple
        String gameID = (String)input.get("gameID");
        String taskID = (String)input.get("taskID");
        String playerID = (String)input.get("playerID");
        String groupID = (String)input.get("groupID");
        String date = (String)input.get("date");
        double score = (Double)input.get("score");
        double timeToComplete = (Double)input.get("timeToComplete");
        
        // We retrieve and store data using mongoDB.
        // There is one collection in the DB for the "traces" and one for the "statistics"
        
        // Step 1: retreive the trialnum by looking at the previous traces
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("performanceStatisticsDB");
        
        // Count all database entries for a specific task in a specific game for a specific player
        long trial = db.getCollection("traces").count();
        System.out.println(trial);
        
        
        /*// Find the database entry for the task-trial combination that is to be analyzed
        
        
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