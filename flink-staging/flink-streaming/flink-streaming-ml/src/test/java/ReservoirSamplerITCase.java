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
