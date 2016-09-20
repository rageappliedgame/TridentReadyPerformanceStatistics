/*
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

import backtype.storm.tuple.Values;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.HashMap;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


// Author (GLA) notes: consider changing this function to State or QueryFunction for
// performance reasons (tuple batch operations)!
public class AddTrialNum implements Function {
    
    // These settings came from KafkaTest.java (they should come from conf in
    // the prepare method but DBUtils keeps giving null pointer exceptions)
    private final String mongoHost = "localhost";
    private final int mongoPort = 27017;
    private final String mongoDB = "gleaner";
    private final String collectionName = "traces";
    private DB db;
    private DBCollection traces; // Connection for this function should be "traces"
        
    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
//        // Extract values from the tuple
//        String playerId;
//        String taskId;
//        
//        // Check database using the values
//        BasicDBObject mongoDoc = new BasicDBObject();
//        mongoDoc.append("test",(Object)"Stuff");
//        
//        //Generate a trial number
//        long trial = traces.count(mongoDoc);
        tridentCollector.emit(new Values(1)); // This currently treats all tuples as the first tuple for that versionId
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        // This method is called whenever this function is started for the first
        // time and the trident system pases the config file here automatically
        try {
            // A connection is established to the mongo db and collection. If
            // the db or collection does not exist, they are generated lazily.
            db = (new MongoClient(mongoHost,mongoPort)).getDB(mongoDB);
            traces = db.getCollection(collectionName);
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
