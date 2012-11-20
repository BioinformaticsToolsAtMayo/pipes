package edu.mayo.pipes.aggregators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.NoSuchElementException;

import com.tinkerpop.pipes.AbstractPipe;

/**
 * Unlike an AggregatorPipe, a Column AggregatorPipe looks at the data to decide if it wants
 * to aggregate information.  For example:
[TTDS00001,Name,Muscarinic acetylcholine receptor,]
[TTDS00001,Type of target,Successful target,]
[TTDS00001,Synonyms,(m)AChR,]
...
[TTDS00001,Antagonist,Hyoscine,DNC000757,]
[TTDS00001,Antagonist,Hyoscyamine sulfate,DNC000758,]
[TTDS00001,Antagonist,Ipratropium bromide,DNC000806,]
[TTDS00001,Agonist,Muscarine,DNC000970,]
[TTDS00001,Agonist,RS 86,DNC001236,]
[TTDS00001,Target Validation,TTDS00001,]
[TTDS00002,UniProt ID,P11229,]
[TTDS00002,Name,Muscarinic acetylcholine receptor M1,]
[TTDS00002,Type of target,Successful target,]
[TTDS00002,Synonyms,M1 receptor,]
...
* 
We can use this pipe to bring together all rows for TTDS00001 and then for TTDS00002
in a second collection and so on...
 * @author dquest
 */

public class ColumnAggregatorPipe extends AbstractPipe<String[], ArrayList<String[]>> {
	private int column = 1;
	private ArrayList objs = new ArrayList();
        private ArrayList prevs = new ArrayList();
        private String prev = "";
	
	public ColumnAggregatorPipe(int column){
		this.column = column;
	}    	
	
	@Override
	protected ArrayList<String[]> processNextStart() throws NoSuchElementException {		
                String[] s;
                objs = new ArrayList();
                if(this.prevs.size()>0){
                    while(!this.prevs.isEmpty()){
                        objs.add(prevs.remove(0));
                    }
                }
		while(true){
                    if(this.starts.hasNext()){
                        s = this.starts.next();
                        //System.out.println(s[column] + ":" + prev);
                        if(s[column].equalsIgnoreCase(prev) || this.objs.size()==0){
                            //System.out.println("true - adding");
                            this.objs.add(s);
                        }else {
                            //System.out.println("Returning Collection...exit(1)");
                            prevs.add(s);
                            prev = s[column];
                            return this.objs;
                        }
                        prev = s[column];
                    }else {
                        //System.out.println("Returning Collection...exit(2)");
                        return this.objs;
                    }
		}
	}

}
