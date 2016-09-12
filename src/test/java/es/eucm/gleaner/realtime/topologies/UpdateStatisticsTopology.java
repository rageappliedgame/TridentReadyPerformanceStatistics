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
package es.eucm.gleaner.realtime.topologies;

import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;

/*
 In this topology two data streams are used to update a game's performance statistics.
 The first stream is the incoming game trace and the second stream is the preexisting
 statistics already present in the database. If there are no preexisting statistics
 a new database entry is created.
 */

public class UpdateStatisticsTopology extends TridentTopology {

	public void prepare() {
		// A topology for the processing of performance statistics is due here

		// TridentTopology topology = new TridentTopology();
		//
		// topology.
	}
}
