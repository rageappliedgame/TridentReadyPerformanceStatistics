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
package es.eucm.gleaner.realtime.functions;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

/*
 * GLA: This function calculates the basic descriptive statistics per task-trial combination
 * based on the level of all observations so far. In the future, functions will
 * be developed that also allow statistics processing on the group and task level.
 */

public class DescriptivesGenerator implements Function {
        
    // These settings came from KafkaTest.java (they should come from conf in
    // the prepare method but DBUtils gives null pointer exceptions)
    private final String mongoHost = "localhost";
    private final int mongoPort = 27017;
    private final String mongoDB = "gleaner";
    private final String collectionName = "performanceStatistics";
    private MongoClient mongoClient;
    private DB db;
    private DBCollection performanceStatistics; // Connection for this function should be "performanceStatistics"
    
    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        // Extract values for searching the DB from the tuple
        Object taskId = objects.getValueByField("target");
        Object trial = objects.getValueByField("trial");
        
        // Extract value for updating the statistics from the tuple
        Double score = Double.parseDouble((String)objects.getValueByField("value"));
        
        /*** Step 1: get the initial performance statistics (from mongoDB or use starting values) ***/
        
        // Generate mongoDB query using target and trial
        BasicDBObject mongoDoc = new BasicDBObject();
        mongoDoc.append("taskId",taskId);
        mongoDoc.append("trial",trial);

        // Prepare statistics variables
        Double max;             // Optional (not required for the calculation of the other statistics)
        Double min;             // Optional (not required for the calculation of the other statistics)
        Double sum;             // Optional (not required for the calculation of the other statistics)
        Double variance;        // Variable needed for the calculation of stdDev
        Double mean;            // Mean (target outcome)
        Double stdDev;          // Standard deviation (target outcome)
        Double skewness;        // The deviation of a gaussian distribution's mean from the median (target outcome)
        Double kurtosis;        // The flatness of a gaussian distribution (target outcome)
        long n;                 // The number of samples that were used in this distribution (number of playthroughs)
        Boolean normal;         // Is the normality assumption respected? (target outcome)
        Double help1;           // Variable needed for skew and kurt calculation
        Double help2;           // Variable needed for skew and kurt calculation
        Double help3;           // Variable needed for skew and kurt calculation
        
        // Query the database in order to find the current statistics status
        n = performanceStatistics.count(mongoDoc);
        
        // Prepare the variables based on the statistics status
        if (n == 0) {
            // There were no previous completions of this task-trial combination so we start with a blank slate
            
            max = 0D;
            min = 0D;
            sum = 0D;
            variance = 0D;
            mean = 0D;
            stdDev = 0D;
            skewness = 0D;
            kurtosis = 0D;
            n = 1;
            normal = false;
            help1 = 0D;
            help2 = 0D;
            help3 = 0D;
            
        } else {
            // There were previous completions of this task-trial combination so we update the previous results
            
            DBCursor currentStatistics = performanceStatistics.find(mongoDoc);
            max = (double)currentStatistics.next().get("max"); // Uses next to get to the first field
            min = (double)currentStatistics.curr().get("min"); // Uses curr to moves through all fields after the first
            sum = (double)currentStatistics.curr().get("sum");
            variance = (double)currentStatistics.curr().get("variance");
            mean = (double)currentStatistics.curr().get("mean");
            stdDev = (double)currentStatistics.curr().get("stdDev");
            skewness = (double)currentStatistics.curr().get("skewness");
            kurtosis = (double)currentStatistics.curr().get("kurtosis");
            n = 1 + (long)currentStatistics.curr().get("n");
            normal = (boolean)currentStatistics.curr().get("normal");
            help1 = (double)currentStatistics.curr().get("help1");
            help2 = (double)currentStatistics.curr().get("help2");
            help3 = (double)currentStatistics.curr().get("help3");
        }
        
        // For testing: output results
//        System.out.print("taskId: "); System.out.print(taskId);System.out.print(", ");
//        System.out.print("trial: "); System.out.print(trial);System.out.print(", ");
//        System.out.print("max: "); System.out.print(max);System.out.print(", ");
//        System.out.print("min: "); System.out.print(min);System.out.print(", ");
//        System.out.print("sum: "); System.out.print(sum);System.out.print(", ");
//        System.out.print("var: "); System.out.print(variance);System.out.print(", ");
//        System.out.print("mea: "); System.out.print(mean);System.out.print(", ");
//        System.out.print("std: "); System.out.print(stdDev);System.out.print(", ");
//        System.out.print("ske: "); System.out.print(skewness);System.out.print(", ");
//        System.out.print("kur: "); System.out.print(kurtosis);System.out.print(", ");
//        System.out.print("n  : "); System.out.print(n);System.out.print(", ");
//        System.out.print("nor: "); System.out.print(normal);System.out.print(", ");
//        System.out.print("hp1: "); System.out.print(help1);System.out.print(", ");
//        System.out.print("hp2: "); System.out.print(help2);System.out.print(", ");
//        System.out.print("hp3: "); System.out.print(help3);System.out.println(".");
        
        /*** Step 2: calculate the up-to-date statistics ***/
        
        if (n == 1) {
            max = score;
            min = score;
            sum = score;
        } else {
            // New max
            if (score > max)
                max = score;

            //New min
            if (score < min)
                min = score;

            // New sum
            sum += score;
        }
        
        // New mean, variance, & stdDev >> based on: http://www.johndcook.com/blog/standard_deviation/
        Double oldMean = mean;
        Double newMean;
        Double oldS = variance;
        Double newS;
        
        if (n == 1) {
            newMean = score;
            newS = 0D;
        } else {
            //New means formula suitable for 1-pass statistics (= big data ready)
            newMean = oldMean + (score - oldMean) / n;
            newS = oldS + (score - oldMean)*(score - newMean);
        }
        
        mean = newMean;
        variance = (n > 1) ? (newS / (n - 1)) : 0D;
        stdDev = Math.sqrt(variance);
        
        
        // New skewness & kurtosis >> based on: http://www.johndcook.com/blog/skewness_kurtosis/
        double delta, delta_n, delta_n2, term1;
        
        delta = score - newMean;
        delta_n = delta / n;
        delta_n2 = delta_n * delta_n;
        term1 = delta * delta_n * n;
        help3 = (term1 * delta_n2 * (n*n-3*n+3))+(6*delta_n2*help1)-(4*delta_n*help2);
        help2 = (term1 * delta_n * (n - 2)) - (3 * n * delta_n * help1);
        help1 = term1;
        
        skewness = (Math.sqrt((double)n) * help2/ Math.pow(help1, 1.5));
        kurtosis = (((double)n)*help3 / (help1*help1) - 3.0);
        
        // For testing: output results
//        System.out.print("taskId: "); System.out.print(taskId);System.out.print(", ");
//        System.out.print("trial: "); System.out.print(trial);System.out.print(", ");
//        System.out.print("max: "); System.out.print(max);System.out.print(", ");
//        System.out.print("min: "); System.out.print(min);System.out.print(", ");
//        System.out.print("sum: "); System.out.print(sum);System.out.print(", ");
//        System.out.print("var: "); System.out.print(variance);System.out.print(", ");
//        System.out.print("mea: "); System.out.print(mean);System.out.print(", ");
//        System.out.print("std: "); System.out.print(stdDev);System.out.print(", ");
//        System.out.print("ske: "); System.out.print(skewness);System.out.print(", ");
//        System.out.print("kur: "); System.out.print(kurtosis);System.out.print(", ");
//        System.out.print("n  : "); System.out.print(n);System.out.print(", ");
//        System.out.print("nor: "); System.out.print(normal);System.out.print(", ");
//        System.out.print("hp1: "); System.out.print(help1);System.out.print(", ");
//        System.out.print("hp2: "); System.out.print(help2);System.out.print(", ");
//        System.out.print("hp3: "); System.out.print(help3);System.out.println(".");
        
        // Step 3: update the mongoDB performanceStatistics collection
        
        // Prepare the new document
        BasicDBObject newMongoDoc = new BasicDBObject();
        newMongoDoc
            .append("taskId",taskId)
            .append("trial",trial)
            .append("max",max)
            .append("min",min)
            .append("sum",sum)
            .append("variance",variance)
            .append("mean",mean)
            .append("stdDev",stdDev)
            .append("skewness",skewness)
            .append("kurtosis",kurtosis)
            .append("n",n)
            .append("normal",normal)
            .append("help1",help1)
            .append("help2",help2)
            .append("help3",help3);
        
        // For EVEN MORE testing
//        System.out.println(newDescriptives);
        
        // Insert the new document (create or update depending on whether it is a new task-trial combination
        if (n == 1){
            // The doc did not previously exist so it will be created
            performanceStatistics.insert(newMongoDoc);
        } else {
            // The doc already existed so update the previous document
            performanceStatistics.update(
                new BasicDBObject()
                    .append("taskId",taskId)
                    .append("trial",trial)    
                , newMongoDoc);
        }
        
        // Step 4: emit the new tuple
        
        // Prepare the tuple
        ArrayList<Object> object = new ArrayList();
        object.add(max);
        object.add(max);
        object.add(min);
        object.add(sum);
        object.add(variance);
        object.add(mean);
        object.add(stdDev);
        object.add(skewness);
        object.add(kurtosis);        
        object.add(n);
        object.add(normal);
        object.add(help1);
        object.add(help2);
        object.add(help3);
        
        // For testing
//        System.out.print("The new tuple is: ");
//        System.out.println(object);
        
        // Emit the tuple
        tridentCollector.emit(object);
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        // This method is called whenever this function is started for the first
        // time and the trident system pases the config file here automatically
        try {
            // A connection is established to the mongo db and collection. If
            // the db or collection does not exist, they are generated lazily.
            mongoClient = new MongoClient();
            db = mongoClient.getDB(mongoDB);
            performanceStatistics = db.getCollection(collectionName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (MongoException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void cleanup() {

    }
}