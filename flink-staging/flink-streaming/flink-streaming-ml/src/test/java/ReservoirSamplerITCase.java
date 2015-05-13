/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.streaming.sampling.examples.StreamApproximationExample;
import org.apache.flink.streaming.util.StreamingProgramTestBase;

/**
 * Created by marthavk on 2015-05-06.
 */

public class ReservoirSamplerITCase extends StreamingProgramTestBase {

	protected String textPath;
	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		//TODO modify preSubmit
		//textPath = createTempFile("text.txt", WordCountData.TEXT);
		//resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		//TODO modify postSubmit
		//compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES, resultPath);
	}

	@Override
	protected void testProgram() throws Exception {
		StreamApproximationExample.main(new String[]{textPath, resultPath});
	}


}
