/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.mayo.pipes.bioinformatics;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.tinkerpop.pipes.AbstractPipe;
import edu.mayo.pipes.bioinformatics.vocab.CoreAttributes;
import edu.mayo.pipes.exceptions.InvalidPipeInputException;
import edu.mayo.pipes.history.ColumnMetaData;
import edu.mayo.pipes.history.History;
import edu.mayo.pipes.util.GenomicObjectUtils;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

/**
 *
 * @author m102417
 * This works to convert a BED file to the 'Golden' Attributes + JSON
 * UCSC has a great description of many common file formats, including BED:
 * https://genome.ucsc.edu/FAQ/FAQformat.html#format1
 * 
 * BED format provides a flexible way to define the data lines that are displayed in an annotation track. BED lines have three required fields and nine additional optional fields. The number of fields per line must be consistent throughout any single set of data in an annotation track. The order of the optional fields is binding: lower-numbered fields must always be populated if higher-numbered fields are used.

If your data set is BED-like, but it is very large and you would like to keep it on your own server, you should use the bigBed data format.

The first three required BED fields are:

    chrom - The name of the chromosome (e.g. chr3, chrY, chr2_random) or scaffold (e.g. scaffold10671).
    chromStart - The starting position of the feature in the chromosome or scaffold. The first base in a chromosome is numbered 0.
    chromEnd - The ending position of the feature in the chromosome or scaffold. The chromEnd base is not included in the display of the feature. For example, the first 100 bases of a chromosome are defined as chromStart=0, chromEnd=100, and span the bases numbered 0-99. 

The 9 additional optional BED fields are:

    name - Defines the name of the BED line. This label is displayed to the left of the BED line in the Genome Browser window when the track is open to full display mode or directly to the left of the item in pack mode.
    score - A score between 0 and 1000. If the track line useScore attribute is set to 1 for this annotation data set, the score value will determine the level of gray in which this feature is displayed (higher numbers = darker gray). This table shows the Genome Browser's translation of BED score values into shades of gray:
    shade 	  	  	  	  	  	  	  	  	 
    score in range   	? 166 	167-277 	278-388 	389-499 	500-611 	612-722 	723-833 	834-944 	? 945
    strand - Defines the strand - either '+' or '-'.
    thickStart - The starting position at which the feature is drawn thickly (for example, the start codon in gene displays).
    thickEnd - The ending position at which the feature is drawn thickly (for example, the stop codon in gene displays).
    itemRgb - An RGB value of the form R,G,B (e.g. 255,0,0). If the track line itemRgb attribute is set to "On", this RBG value will determine the display color of the data contained in this BED line. NOTE: It is recommended that a simple color scheme (eight colors or less) be used with this attribute to avoid overwhelming the color resources of the Genome Browser and your Internet browser.
    blockCount - The number of blocks (exons) in the BED line.
    blockSizes - A comma-separated list of the block sizes. The number of items in this list should correspond to blockCount.
    blockStarts - A comma-separated list of block starts. All of the blockStart positions should be calculated relative to chromStart. The number of items in this list should correspond to blockCount. 

Example:
Here's an example of an annotation track that uses a complete BED definition:

track name=pairedReads description="Clone Paired Reads" useScore=1
chr22 1000 5000 cloneA 960 + 1000 5000 0 2 567,488, 0,3512
chr22 2000 6000 cloneB 900 - 2000 6000 0 2 433,399, 0,3601

Example:
This example shows an annotation track that uses the itemRgb attribute to individually color each data line. In this track, the color scheme distinguishes between items named "Pos*" and those named "Neg*". See the usage note in the itemRgb description above for color palette restrictions. NOTE: The track and data lines in this example have been reformatted for documentation purposes. This example can be pasted into the browser without editing.

browser position chr7:127471196-127495720
browser hide all
track name="ItemRGBDemo" description="Item RGB demonstration" visibility=2
itemRgb="On"
chr7    127471196  127472363  Pos1  0  +  127471196  127472363  255,0,0
chr7    127472363  127473530  Pos2  0  +  127472363  127473530  255,0,0
chr7    127473530  127474697  Pos3  0  +  127473530  127474697  255,0,0
chr7    127474697  127475864  Pos4  0  +  127474697  127475864  255,0,0
chr7    127475864  127477031  Neg1  0  -  127475864  127477031  0,0,255
chr7    127477031  127478198  Neg2  0  -  127477031  127478198  0,0,255
chr7    127478198  127479365  Neg3  0  -  127478198  127479365  0,0,255
chr7    127479365  127480532  Pos5  0  +  127479365  127480532  255,0,0
chr7    127480532  127481699  Neg4  0  -  127480532  127481699  0,0,255

Click here to display this track in the Genome Browser.

Example:
It is also possible to color items by strand in a BED track using the colorByStrand attribute in the track line as shown below. For BED tracks, this attribute functions only for custom tracks with 6 to 8 fields (i.e. BED6 through BED8). NOTE: The track and data lines in this example have been reformatted for documentation purposes. This example can be pasted into the browser without editing.

browser position chr7:127471196-127495720
browser hide all
track name="ColorByStrandDemo" description="Color by strand demonstration"
visibility=2 colorByStrand="255,0,0 0,0,255"
chr7    127471196  127472363  Pos1  0  +
chr7    127472363  127473530  Pos2  0  +
chr7    127473530  127474697  Pos3  0  +
chr7    127474697  127475864  Pos4  0  +
chr7    127475864  127477031  Neg1  0  -
chr7    127477031  127478198  Neg2  0  -
chr7    127478198  127479365  Neg3  0  -
chr7    127479365  127480532  Pos5  0  +
chr7    127480532  127481699  Neg4  0  -

 * 
 * 
 * Note, if fields in the BED file are not filled in, then JSON will just not contain the keys
 */
public class BED2JSONPipe extends AbstractPipe<History,History> {

    private static final Logger sLogger = Logger.getLogger(BED2JSONPipe.class);
    // number of data line (does not include header lines)
    private int mDataLineNumber = 1;
    
    private History doHeader(History h){
        List<ColumnMetaData> columns = History.getMetaData().getColumns();
        //remove column headers called "#UNKNOWN"
        
        if((columns.size() > 2 && h.size() > 2)){
            //add the required columns
            ColumnMetaData cmd = new ColumnMetaData("chrom");
            columns.set(0, cmd);
            cmd = new ColumnMetaData("chromStart");
            columns.set(1, cmd);
            cmd = new ColumnMetaData("chromEnd");
            columns.set(2, cmd);
            //assume if the column is there than it is the next bed field.
            if(h.size() > 3){
                cmd = new ColumnMetaData("name");
                columns.set(3, cmd);
            }
            if(h.size() > 4){
                cmd = new ColumnMetaData("score");
                columns.set(4, cmd);
            }
            if(h.size() > 5){
                cmd = new ColumnMetaData("strand");
                columns.set(5, cmd);
            } 
            if(h.size() > 6){
                cmd = new ColumnMetaData("thickStart");
                columns.set(6, cmd);
            } 
            if(h.size() > 7){
                cmd = new ColumnMetaData("thickEnd");
                columns.set(7, cmd);
            } 
            if(h.size() > 8){
                cmd = new ColumnMetaData("itemRgb");
                columns.set(8, cmd);
            } 
            if(h.size() > 9){
                cmd = new ColumnMetaData("blockCount");
                columns.set(9, cmd);
            }
            if(h.size() > 10){
                cmd = new ColumnMetaData("blockSizes");
                columns.set(10, cmd);
            } 
            if(h.size() > 11){
                cmd = new ColumnMetaData("blockStarts");
                columns.set(11, cmd);
            }

        }
        return h;
    }
    
    private boolean first = true;
    @Override
    protected History processNextStart() throws NoSuchElementException {
        History h = this.starts.next();
                // check to make sure we have the required minimum # of columns
        if(h.size() < 3){
        	final int requiredColCount = 3;
        	final int actualColCount = h.size();
        	
        	StringBuilder sb = new StringBuilder();
        	sb.append("Invalid BED data line at data line # %s.\n");
        	sb.append("The BED format requires >2 fixed fields per data line, but found only %s field(s).\n");
        	sb.append("Make sure the BED file has the necessary %s BED fields delimited by TAB characters.\n");
        	sb.append("Invalid BED line content: \"%s\"");
        	
        	String errorMesg = String.format(
        							sb.toString(),
        							String.valueOf(mDataLineNumber),
        							requiredColCount,
        							actualColCount,
        							requiredColCount,
        							h.getMergedData("\t")
        						);     	
        	throw new InvalidPipeInputException(errorMesg, this);
      	}
        //note, BED files don't require a header.  if it is there, this pipe will add to it, if not, it will add one.
        if(first){
            h = doHeader(h);
        }
        first = false;
        // record the data line we are going to process
        h.add(createJson(h).toString());
        mDataLineNumber++;
        return h;
    }
    
    private JsonObject createJson(History h){
        JsonObject json = new JsonObject();
        //chrom
        json.addProperty("chrom", h.get(0));
        String chr = GenomicObjectUtils.computechr(h.get(0).trim());
    	json.addProperty(CoreAttributes._landmark.toString(), chr);
        //minbp
        json.addProperty("chromStart", h.get(1).trim());
    	json.addProperty(CoreAttributes._minBP.toString(), new Integer(h.get(1))+1);//UCSC is 0 based, need to convert it up 1.
        //maxbp
        json.addProperty("chromEnd", h.get(2).trim());
    	json.addProperty(CoreAttributes._maxBP.toString(), new Integer(h.get(2)));//UCSC is 0 based, need to convert it up 1.

        //assume if the column is there than it is the next bed field.
        if(h.size() > 3){
            json.addProperty("name", h.get(3).trim());
        }
        if(h.size() > 4){
            json.addProperty("score", h.get(4).trim());
        }
        if(h.size() > 5){
            json.addProperty("strand", h.get(5).trim());
        } 
        if(h.size() > 6){
            json.addProperty("thickStart", h.get(6).trim());
        } 
        if(h.size() > 7){
            json.addProperty("thickEnd", h.get(7).trim());
        } 
        if(h.size() > 8){
            json.addProperty("itemRgb", h.get(8).trim());
        } 
        if(h.size() > 9){
            json.addProperty("blockCount", h.get(9).trim());
        }
        if(h.size() > 10){
            json.addProperty("blockSizes", h.get(10).trim());
        } 
        if(h.size() > 11){
            json.addProperty("blockStarts", h.get(11).trim());
        }
        return json;
        
    }
    
}
