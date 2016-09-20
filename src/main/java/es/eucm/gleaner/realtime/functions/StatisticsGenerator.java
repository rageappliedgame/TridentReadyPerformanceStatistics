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

import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.Map;

public class StatisticsGenerator implements Function {
        
        @Override
	public void execute(TridentTuple objects, TridentCollector tridentCollector) {
            System.out.print("Tuple with trial added: ");
            System.out.println(objects);
        }

	@Override
	public void prepare(Map conf, TridentOperationContext context) {
            // This method is called whenever this function is started for the first
            // time and the trident system pases the config file here automatically
            
	}

	@Override
	public void cleanup() {

	}
}