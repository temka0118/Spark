package kAnonymity;

import java.util.List;

import org.apache.spark.Partitioner;

class GreedyPartitioner extends Partitioner{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int numParts;
	private List<Integer> medians;
	
	public GreedyPartitioner(int i, List<Integer> medians) {
		numParts=i;
		this.medians = medians;
	}

	@Override
	public int numPartitions()
	{
	    return numParts;
	}

	@Override
	public int getPartition(Object key){
		 
		 int keyVal = Integer.valueOf(key.toString());
		 int i = 0;
		 for (i = 0; i < medians.size(); i++) {
			 if( keyVal <= medians.get(i) ) {
				 return i;
			 }
		 }
		 return i;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof GreedyPartitioner)
		{
			GreedyPartitioner partitionerObject = (GreedyPartitioner)obj;
			if(partitionerObject.numParts == this.numParts)
				return true;
		}
	
		return false;
	   }
}