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

import backtype.storm.Config;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import es.eucm.gleaner.realtime.utils.DBUtils;
import java.net.UnknownHostException;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class AddTrialNum implements Function {
	
    private final String mongoHost;
    private final String mongoPort;
    private final String mongoDB;
    
    public AddTrialNum (String host, String port, String db) {
        // This method is called once whenever the topology creates a stream
        // that uses this function (as tested by the System.out.printlns below)
        mongoHost = host;
        mongoPort = port;
        mongoDB = db;
        
//        System.out.println(mongoHost);
//        System.out.println(mongoPort);
//        System.out.println(mongoDB);
    }
    
    @Override
    public void execute(TridentTuple objects, TridentCollector tridentCollector) {
        // Take the gameplayId and the versionId to be able to seach the mongoDB for traces
//        Map gameplayId = (Map) objects.getValueByField("gameplayId");
//        Map versionId = (Map) objects.getValueByField("versionId");
//        
//        System.out.println(gameplayId.get("gameplayId"));
//        System.out.println(versionId.get("versionId"));
        System.out.println(mongoHost);
        System.out.println(mongoPort);
        System.out.println(mongoDB);

        // Search and count the tracesDB in order to calculate the trialNum
        /*try {
            // Connect to the mongoDB

            //My current problem? Getting the address of the mongoDB host, the port, and the db name

        } catch (UnknownHostException e) {
                e.printStackTrace();
        } catch (MongoException e) {
                e.printStackTrace();
        }*/

        // Append the result to the previously existing tuple
        tridentCollector.emit(Arrays.asList(1));
    }

    @Override
    public void prepare(Map map, TridentOperationContext tridentOperationContext) {
        // Not sure when this method is ever called :(
    }

    @Override
    public void cleanup() {

    }
}
