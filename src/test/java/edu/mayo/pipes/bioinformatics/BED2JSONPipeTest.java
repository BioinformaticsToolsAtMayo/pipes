package edu.mayo.pipes.bioinformatics;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.metadata.Metadata;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: m102417
 * Date: 8/7/13
 * Time: 4:20 PM
 * To change this template use File | Settings | File Templates.
 */
public class BED2JSONPipeTest {

    public final List<String> input = Arrays.asList(
    		"#chrom\tchromStart\tchromEnd\tname\tscore\tstrand\tthickStart\tthickEnd\titemRgb",
            "chr7\t127471196\t127472363\tPos1\t0\t+\t127471196\t127472363\t255,0,0" ,
            "chr7\t127472363\t127473530\tPos2\t0\t+\t127472363\t127473530\t255,0,0",
            "chr7\t127473530\t127474697\tPos3\t0\t+\t127473530\t127474697\t255,0,0",
            "chr7\t127474697\t127475864\tPos4\t0\t+\t127474697\t127475864\t255,0,0",
            "chr7\t127475864\t127477031\tNeg1\t0\t-\t127475864\t127477031\t0,0,255",
            "chr7\t127477031\t127478198\tNeg2\t0\t-\t127477031\t127478198\t0,0,255",
            "chr7\t127478198\t127479365\tNeg3\t0\t-\t127478198\t127479365\t0,0,255",
            "chr7\t127479365\t127480532\tPos5\t0\t+\t127479365\t127480532\t255,0,0",
            "chr7\t127480532\t127481699\tNeg4\t0\t-\t127480532\t127481699\t0,0,255"
    );

    public final List<String> out = Arrays.asList(
            "##BIOR=<ID=\"bior.ToTJson\",Operation=\"bed_to_tjson\",DataType=\"JSON\",ShortUniqueName=\"ToTJson\">",
            "#chrom\tchromStart\tchromEnd\tname\tscore\tstrand\tthickStart\tthickEnd\titemRgb\tbior.ToTJson",
            "chr7\t127471196\t127472363\tPos1\t0\t+\t127471196\t127472363\t255,0,0\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127471196\",\"_minBP\":127471197,\"chromEnd\":\"127472363\",\"_maxBP\":127472363,\"name\":\"Pos1\",\"score\":\"0\",\"strand\":\"+\",\"thickStart\":\"127471196\",\"thickEnd\":\"127472363\",\"itemRgb\":\"255,0,0\"}",
            "chr7\t127472363\t127473530\tPos2\t0\t+\t127472363\t127473530\t255,0,0\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127472363\",\"_minBP\":127472364,\"chromEnd\":\"127473530\",\"_maxBP\":127473530,\"name\":\"Pos2\",\"score\":\"0\",\"strand\":\"+\",\"thickStart\":\"127472363\",\"thickEnd\":\"127473530\",\"itemRgb\":\"255,0,0\"}",
            "chr7\t127473530\t127474697\tPos3\t0\t+\t127473530\t127474697\t255,0,0\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127473530\",\"_minBP\":127473531,\"chromEnd\":\"127474697\",\"_maxBP\":127474697,\"name\":\"Pos3\",\"score\":\"0\",\"strand\":\"+\",\"thickStart\":\"127473530\",\"thickEnd\":\"127474697\",\"itemRgb\":\"255,0,0\"}",
            "chr7\t127474697\t127475864\tPos4\t0\t+\t127474697\t127475864\t255,0,0\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127474697\",\"_minBP\":127474698,\"chromEnd\":\"127475864\",\"_maxBP\":127475864,\"name\":\"Pos4\",\"score\":\"0\",\"strand\":\"+\",\"thickStart\":\"127474697\",\"thickEnd\":\"127475864\",\"itemRgb\":\"255,0,0\"}",
            "chr7\t127475864\t127477031\tNeg1\t0\t-\t127475864\t127477031\t0,0,255\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127475864\",\"_minBP\":127475865,\"chromEnd\":\"127477031\",\"_maxBP\":127477031,\"name\":\"Neg1\",\"score\":\"0\",\"strand\":\"-\",\"thickStart\":\"127475864\",\"thickEnd\":\"127477031\",\"itemRgb\":\"0,0,255\"}",
            "chr7\t127477031\t127478198\tNeg2\t0\t-\t127477031\t127478198\t0,0,255\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127477031\",\"_minBP\":127477032,\"chromEnd\":\"127478198\",\"_maxBP\":127478198,\"name\":\"Neg2\",\"score\":\"0\",\"strand\":\"-\",\"thickStart\":\"127477031\",\"thickEnd\":\"127478198\",\"itemRgb\":\"0,0,255\"}",
            "chr7\t127478198\t127479365\tNeg3\t0\t-\t127478198\t127479365\t0,0,255\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127478198\",\"_minBP\":127478199,\"chromEnd\":\"127479365\",\"_maxBP\":127479365,\"name\":\"Neg3\",\"score\":\"0\",\"strand\":\"-\",\"thickStart\":\"127478198\",\"thickEnd\":\"127479365\",\"itemRgb\":\"0,0,255\"}",
            "chr7\t127479365\t127480532\tPos5\t0\t+\t127479365\t127480532\t255,0,0\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127479365\",\"_minBP\":127479366,\"chromEnd\":\"127480532\",\"_maxBP\":127480532,\"name\":\"Pos5\",\"score\":\"0\",\"strand\":\"+\",\"thickStart\":\"127479365\",\"thickEnd\":\"127480532\",\"itemRgb\":\"255,0,0\"}",
            "chr7\t127480532\t127481699\tNeg4\t0\t-\t127480532\t127481699\t0,0,255\t{\"chrom\":\"chr7\",\"_landmark\":\"7\",\"chromStart\":\"127480532\",\"_minBP\":127480533,\"chromEnd\":\"127481699\",\"_maxBP\":127481699,\"name\":\"Neg4\",\"score\":\"0\",\"strand\":\"-\",\"thickStart\":\"127480532\",\"thickEnd\":\"127481699\",\"itemRgb\":\"0,0,255\"}"
    );

    @Test
    public void testBEDwithMetadata(){
        Metadata md = new Metadata("bed_to_tjson");
        Pipeline p = new Pipeline(new HistoryInPipe(md), new BED2JSONPipe(), new HistoryOutPipe());
        p.setStarts(input);
        for(int i=0; p.hasNext(); i++){
            String line = (String) p.next();
            System.out.println(line);
            assertEquals(out.get(i), line);
        }



    }


}
