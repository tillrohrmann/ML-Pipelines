/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.examples.unifiedStreamBatch.helpers;


import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.flink.api.java.tuple.Tuple;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Utils {

	private static String COMMA_DELIMITER = ",";
	private static String NEW_LINE_SEPARATOR = "\n";
	private static String TEXT_FILE_DELIMITER = "\t";

	public Utils() {
	}

	public void dataSetToTextFile(List<Tuple> dataSet, String filename) {

		try {
			File file = new File(filename);
			BufferedWriter output = new BufferedWriter(new FileWriter(file));
			for (int i = 0; i < dataSet.size(); i++) {
				for (int j = 0; j < dataSet.get(i).getArity(); j++) {
					output.write(dataSet.get(i).getField(j).toString());
					output.write(TEXT_FILE_DELIMITER);
				}
				output.write(NEW_LINE_SEPARATOR);
			}
			output.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void dataSetToCSVFile(List<Tuple> ds, String filename) {
		//TODO::erase or modify this function
		FileWriter fw = null;
		CSVPrinter cp = null;
		CSVFormat format = CSVFormat.DEFAULT.withRecordSeparator('\n');

		try {
			fw = new FileWriter(filename);
			cp = new CSVPrinter(fw, format);
			for (Tuple dataPoint : ds) {
				List data = new ArrayList();
				for (int j = 0; j < dataPoint.getArity(); j++) {
					data.add(dataPoint.getField(0).toString());
				}
				cp.printRecord(data);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {

				fw.close();
				cp.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	public void writeCSV(List<Tuple> ds, String filename) {
		FileWriter fw = null;

		try {
			fw = new FileWriter(filename);
			for (int j = 0; j < ds.size(); j++) {
				for (int k = 0; k < ds.get(j).getArity(); k++) {
					fw.append(ds.get(j).getField(k).toString());
					//don't place the comma delimiter at the end of each line
					if (!(k == ds.get(j).getArity() - 1)) {
						fw.append(COMMA_DELIMITER);
					}
				}
				fw.append(NEW_LINE_SEPARATOR);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
		}
	}
}
