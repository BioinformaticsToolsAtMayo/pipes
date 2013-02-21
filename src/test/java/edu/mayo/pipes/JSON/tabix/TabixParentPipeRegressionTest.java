/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.JSON.tabix;

import com.tinkerpop.pipes.util.Pipeline;
import edu.mayo.pipes.JSON.lookup.LookupPipe;
import edu.mayo.pipes.MergePipe;
import edu.mayo.pipes.PrintPipe;
import edu.mayo.pipes.history.HistoryInPipe;
import java.io.IOException;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author m102417
 */
public class TabixParentPipeRegressionTest {
    
    public TabixParentPipeRegressionTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }
    
    @Test
    public void testRegression() throws IOException{
        String dbIndexFile = "src/test/resources/testData/tabix/index/genes.HGNC.idx.h2.db";
        String catalog = "src/test/resources/testData/tabix/genes.tsv.bgz";
        String[] lines = {
            "1	123	456	18500", 
            "X	123	456	23270",
            "Y	123	456	.",	
            "22	123	456	38436",
            "22	123	456	6030"
        };
        String[] results = {
            "1	123	456	18500	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28740815,\"_maxBP\":28780802,\"gene\":\"PARP4P1\",\"gene_synonym\":\"ADPRTL1P; PARP4P; PARP4PY1\",\"note\":\"poly (ADP-ribose) polymerase family, member 4 pseudogene 1; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"347613\",\"HGNC\":\"18500\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28740815,\"_maxBP\":28780802,\"gene\":\"PARP4P1\",\"gene_synonym\":\"ADPRTL1P; PARP4P; PARP4PY1\",\"note\":\"poly (ADP-ribose) polymerase family, member 4 pseudogene 1; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"347613\",\"HGNC\":\"18500\"}",
            "1	123	456	18500	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28740815,\"_maxBP\":28780802,\"gene\":\"PARP4P1\",\"gene_synonym\":\"ADPRTL1P; PARP4P; PARP4PY1\",\"note\":\"poly (ADP-ribose) polymerase family, member 4 pseudogene 1; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"347613\",\"HGNC\":\"18500\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28772670,\"_maxBP\":28773393,\"gene\":\"FAM58CP\",\"gene_synonym\":\"FAM58AP1; FAM58Y\",\"note\":\"family with sequence similarity 58, member C, pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"100421487\",\"HGNC\":\"38436\"}",
            "X	123	456	23270	{\"_type\":\"gene\",\"_landmark\":\"X\",\"_strand\":\"-\",\"_minBP\":155215011,\"_maxBP\":155215912,\"gene\":\"TRPC6P\",\"gene_synonym\":\"TRPC6L\",\"note\":\"transient receptor potential cation channel, subfamily C, member 6 pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"644218\",\"HGNC\":\"23270\"}	{\"_type\":\"gene\",\"_landmark\":\"X\",\"_strand\":\"-\",\"_minBP\":155215011,\"_maxBP\":155215912,\"gene\":\"TRPC6P\",\"gene_synonym\":\"TRPC6L\",\"note\":\"transient receptor potential cation channel, subfamily C, member 6 pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"644218\",\"HGNC\":\"23270\"}",
            "X	123	456	23270	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":59318017,\"_maxBP\":59318918,\"gene\":\"TRPC6P\",\"gene_synonym\":\"TRPC6L\",\"note\":\"transient receptor potential cation channel, subfamily C, member 6 pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"644218\",\"HGNC\":\"23270\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":59318017,\"_maxBP\":59318918,\"gene\":\"TRPC6P\",\"gene_synonym\":\"TRPC6L\",\"note\":\"transient receptor potential cation channel, subfamily C, member 6 pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"644218\",\"HGNC\":\"23270\"}",
            "Y	123	456	.	{}	{}",
            "22	123	456	38436	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28772670,\"_maxBP\":28773393,\"gene\":\"FAM58CP\",\"gene_synonym\":\"FAM58AP1; FAM58Y\",\"note\":\"family with sequence similarity 58, member C, pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"100421487\",\"HGNC\":\"38436\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28740815,\"_maxBP\":28780802,\"gene\":\"PARP4P1\",\"gene_synonym\":\"ADPRTL1P; PARP4P; PARP4PY1\",\"note\":\"poly (ADP-ribose) polymerase family, member 4 pseudogene 1; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"347613\",\"HGNC\":\"18500\"}",
            "22	123	456	38436	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28772670,\"_maxBP\":28773393,\"gene\":\"FAM58CP\",\"gene_synonym\":\"FAM58AP1; FAM58Y\",\"note\":\"family with sequence similarity 58, member C, pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"100421487\",\"HGNC\":\"38436\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"-\",\"_minBP\":28772670,\"_maxBP\":28773393,\"gene\":\"FAM58CP\",\"gene_synonym\":\"FAM58AP1; FAM58Y\",\"note\":\"family with sequence similarity 58, member C, pseudogene; Derived by automated computational analysis using gene prediction method: Curated Genomic.\",\"pseudo\":\"\",\"GeneID\":\"100421487\",\"HGNC\":\"38436\"}",
            "22	123	456	6030	{\"_type\":\"gene\",\"_landmark\":\"X\",\"_strand\":\"+\",\"_minBP\":155227246,\"_maxBP\":155240482,\"gene\":\"IL9R\",\"gene_synonym\":\"CD129; IL-9R\",\"note\":\"interleukin 9 receptor; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"3581\",\"HGNC\":\"6030\",\"HPRD\":\"02052\",\"MIM\":\"300007\"}	{\"_type\":\"gene\",\"_landmark\":\"X\",\"_strand\":\"+\",\"_minBP\":155227246,\"_maxBP\":155240482,\"gene\":\"IL9R\",\"gene_synonym\":\"CD129; IL-9R\",\"note\":\"interleukin 9 receptor; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"3581\",\"HGNC\":\"6030\",\"HPRD\":\"02052\",\"MIM\":\"300007\"}",
            "22	123	456	6030	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"+\",\"_minBP\":59330252,\"_maxBP\":59343488,\"gene\":\"IL9R\",\"gene_synonym\":\"CD129; IL-9R\",\"note\":\"interleukin 9 receptor; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"3581\",\"HGNC\":\"6030\",\"HPRD\":\"02052\",\"MIM\":\"300007\"}	{\"_type\":\"gene\",\"_landmark\":\"Y\",\"_strand\":\"+\",\"_minBP\":59330252,\"_maxBP\":59343488,\"gene\":\"IL9R\",\"gene_synonym\":\"CD129; IL-9R\",\"note\":\"interleukin 9 receptor; Derived by automated computational analysis using gene prediction method: BestRefseq.\",\"GeneID\":\"3581\",\"HGNC\":\"6030\",\"HPRD\":\"02052\",\"MIM\":\"300007\"}"
        };
        Pipeline p = new Pipeline(
                new HistoryInPipe(),
                new LookupPipe(dbIndexFile,catalog), 
                new OverlapPipe(catalog),
                new MergePipe("\t")//,
                //new PrintPipe()
                );
        p.setStarts(Arrays.asList(lines));
        for(int i=0; p.hasNext(); i++){
            String result = (String) p.next();
            assertEquals(result, results[i]);
            //note matches stop after the 3rd to last or so :(
        }
    }

}
