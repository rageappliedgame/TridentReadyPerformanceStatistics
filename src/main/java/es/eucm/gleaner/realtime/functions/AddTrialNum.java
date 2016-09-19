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

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.Map;
import static org.apache.storm.http.client.methods.RequestBuilder.options;

public class AddTrialNum implements Function {
    
    // These settings came from KafkaTest.java (they should come from conf in
    // the prepare method but DBUtils keeps giving null pointer exceptions)
    private String mongoHost = "localhost";
    private int mongoPort = 27017;
    private String mongoDB = "gleaner";
    private DB db;
    private String collectionName = "traces";
    private DBCollection traces; // Connection for this function should be "traces"
        
    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        BasicDBObject mongoDoc = new BasicDBObject();
        mongoDoc.append("test",(Object)"Stuff");
        long count = traces.count(mongoDoc);
        
        // For testing
        System.out.print("There were this many documents in mongo: ");
        System.out.println(count);
        // Append the result to the previously existing tuple
        tridentCollector.emit(Arrays.asList(1)); // This currently treats all tuples as the first tuple for that versionId
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
