/**
 * Copyright (C) 2016 e-UCM (http://www.e-ucm.es/)
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

import storm.trident.operation.Filter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

// ### Comments by Giel van Lankveld (OUNL) ###
//
// This class mimics the class "WriteMongoFilter".
// If you can call this class successfully your code should also be able
// to successfully call the class responsible for outputting analytics results
// to the MongoDB.
// 
// This class also mimics the class "FieldValueFilter" but I have no clue what
// that class is supposed to do.

public class WriteToTextFile implements Filter {

	private String textField;

	private String[] fields;

	public WriteToTextFile(String textField, String... fields) {

		// Comment by Giel van Lankveld (OUNL):
		// This method stores the filename to be written in 'textField'
		// and the output that is to be written is contained in 'fields' (which
		// is an array of strings).
		// Fields should contain a list of the values to be output later on.

		this.textField = textField;
		this.fields = fields;
	}

	@Override
	public boolean isKeep(TridentTuple tuple) {

		// Comment by Giel van Lankveld (OUNL):
		// When this method is called, the contents of fields are written to the
		// file textField (I think).

		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(
					textField, true));
			for (String field : fields) {
				// Comment by Giel van Lankveld (OUNL):
				// Writes tuples of the values contained in the string array
				// "fields"
				writer.write(tuple.getValueByField(field) + ",");
			}
			writer.newLine();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	@Override
	public void prepare(Map conf, TridentOperationContext context) {

	}

	@Override
	public void cleanup() {

	}
}
