package edu.mayo.pipes.functional;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.DrillPipe;
import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.UNIX.CatPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import edu.mayo.pipes.history.HistoryOutPipe;
import edu.mayo.pipes.util.metadata.Metadata;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class LookupMetaFunctional {

    public List<String> historyLookupOut = Arrays.asList(
            "##BIOR=<ID=\"bior.genes\",Operation=\"bior_lookup\",DataType=\"JSON\",ShortUniqueName=\"genes\",Path=\"src/test/resources/testData/tabix/genes.tsv.bgz\">",
            "#UNKNOWN_1\tbior.genes",
            "BRCA1\t{\"_type\":\"gene\",\"_landmark\":\"17\",\"_strand\":\"-\",\"_minBP\":41196312,\"_maxBP\":41277500,\"gene\":\"BRCA1\",\"gene_synonym\":\"BRCAI; BRCC1; BROVCA1; IRIS; PNCA4; PPP1R53; PSCP; RNF53\",\"note\":\"breast cancer 1, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"672\",\"HGNC\":\"1100\",\"HPRD\":\"00218\",\"MIM\":\"113705\"}",
            "BRCA2\t{\"_type\":\"gene\",\"_landmark\":\"13\",\"_strand\":\"+\",\"_minBP\":32889617,\"_maxBP\":32973809,\"gene\":\"BRCA2\",\"gene_synonym\":\"BRCC2; BROVCA2; FACD; FAD; FAD1; FANCB; FANCD; FANCD1; GLM3; PNCA2\",\"note\":\"breast cancer 2, early onset; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"675\",\"HGNC\":\"1101\",\"HPRD\":\"02554\",\"MIM\":\"600185\"}",
            "MTHFR\t{\"_type\":\"gene\",\"_landmark\":\"1\",\"_strand\":\"-\",\"_minBP\":11845787,\"_maxBP\":11866160,\"gene\":\"MTHFR\",\"note\":\"methylenetetrahydrofolate reductase (NAD(P)H); Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"4524\",\"HGNC\":\"7436\",\"HPRD\":\"06158\",\"MIM\":\"607093\"}"
    );


    String catalog = "src/test/resources/testData/tabix/genes.tsv.bgz";
    String index = "src/test/resources/testData/tabix/index/genes.gene.idx.h2.db";
    @Test
    public void testHistoryInDrillHistoryOut(){
        Metadata md = new Metadata(catalog, "bior_lookup");
        Pipeline p = new Pipeline(
                new HistoryInPipe(md),
                new LookupPipe(catalog, index),
                new HistoryOutPipe()
        );
        p.setStarts(Arrays.asList("BRCA1", "BRCA2", "MTHFR"));
        for(int i=0; p.hasNext(); i++){
            String out = (String) p.next();
            //System.out.println(out);
            assertEquals(historyLookupOut.get(i), out);
            if(i==4)break;
        }
    }
}
