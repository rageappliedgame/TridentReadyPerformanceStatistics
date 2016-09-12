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

import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class UpdateStatisticsFunction extends BaseFunction {

	// Maybe my function should establish the needed database connections on
	// startup?

	public void updateStatisticsRecord(HashMap newTrace) { // playerID,taskID,score,ttc
		// In this method we update the performance statistics record in mongoDB

		String taskID = (String) newTrace.get("taskId");
		String score = (String) newTrace.get("score");

		// Code for the apache storm version
		// String taskID = (String)newTrace.getValueByField("taskID");
		// String score = (String)newTrace.getValueByField("score");

		// Search the traces DB to find the trial number
		// Long trialNum = ...

		// Prepare variable used in the the analysis
		double oldMean, newMean;
		double oldSD, newSD;
		double oldSkew, newSkew;
		double oldKurt, newKurt;
		long oldN, newN;
		double oldSSE, newSSE;
		double oldSum, newSum;

		// Get the relevant data from mongoDB
		try {
			// Connect to the mongoDB
			MongoClient mongo = new MongoClient("localhost", 27017);
			List<String> dbNames = mongo.getDatabaseNames();

			// Check if the gameplay trace DB already existed
			if (dbNames.contains((String) "GameplayTraceDB")) {
				// The db existed so retrieve the relevant data

			} else {
				// The db did not exist so assume the trace is the first case
				// ever and create the DB

			}

			// Check if the statistics DB already existed
			if (dbNames.contains((String) "PerformanceStatisticsDB")) {
				// The db existed so update it

			} else {
				// The db did not exist so assume the trace is the first case
				// ever
				oldMean = Double.parseDouble(score);
				oldSD = 0;
			}

		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (MongoException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// Incoming tuple should contain the fields: tracePlayerID, traceTaskID,
		// score, ttc,
		// taskID, trial, mean, SD, skew, kurt, count, sum, and SSE.
		// The goal of this function is to update the final nine fields using
		// the scores of the first four fields.

	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
