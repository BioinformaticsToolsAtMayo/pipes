package edu.mayo.pipes.functional;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.UNIX.CatPipe;
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
 * Date: 8/6/13
 * Time: 10:14 AM
 * To change this template use File | Settings | File Templates.
 *
 *
 * Functional test to make sure that Drill is correctly working with Metadata
 *
 */
public class DrillMetaFunctional {

    public List<String> historyDrillOut = Arrays.asList(
            "#UNKNOWN_1\t#UNKNOWN_2\t#UNKNOWN_3\tbior.#UNKNOWN_4.INFO.RSPOS\tbior.#UNKNOWN_4.INFO.dbSNPBuildID",
            "1\t10144\t10145\t10145\t134",
            "1\t10177\t10177\t10177\t137",
            "1\t10180\t10180\t10180\t137"
    );

    /**
     * Functional test showed this error:
     *
     $ gunzip -c src/test/resources/metadata/00-All_GRCh37.tsv.bgz | bior_drill -p INFO.RSPOS -p INFO.dbSNPBuildID --log
     Error executing bior_drill

     Internal system error.
     *
     *
     *
     */

    @Test
    public void testHistoryInDrillHistoryOut(){
        String[] paths = new String[]{"INFO.RSPOS","INFO.dbSNPBuildID"};
        Metadata md = new Metadata(Metadata.CmdType.Drill, -1, "bior_drill", false, paths);
        Pipeline p = new Pipeline(
                new CatPipe(),
                new HistoryInPipe(md),
                new DrillPipe(false, paths),
                new HistoryOutPipe()
        );
        p.setStarts(Arrays.asList("src/test/resources/testData/metadata/00-All_GRCh37.tsv.bgz"));
        for(int i=0; p.hasNext(); i++){
            String out = (String) p.next();
            System.out.println(out);
            assertEquals(historyDrillOut.get(i), out);
            if(i==3)break;
        }
    }


}
