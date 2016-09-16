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

import backtype.storm.Config;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class StatisticsGenerator implements Function {
        
        @Override
	public void execute(TridentTuple objects, TridentCollector tridentCollector) {
            // Take the gameplayId and the versionId to be able to seach the mongoDB for traces
            Map gameplayId = (Map) objects.getValueByField("gameplayId");
            Map versionId = (Map) objects.getValueByField("versionId");
//            Object temp = conf.get("mongoHost");
//            String host = (String)temp;
//            System.out.println(host);

            // Search and count the tracesDB in order to calculate the trialNum
            /*try {
                // Connect to the mongoDB

                //My current problem? Getting the address of the mongoDB host, the port, and the db name

            } catch (UnknownHostException e) {
                    e.printStackTrace();
            } catch (MongoException e) {
                    e.printStackTrace();
            }

            // Append the result to the previously existing tuple
            if (value != null) {
                    tridentCollector.emit(Arrays.asList(trialNum));
            }*/
        }

	@Override
	public void prepare(Map map, TridentOperationContext tridentOperationContext) {

	}

	@Override
	public void cleanup() {

	}
}