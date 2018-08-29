package kAnonymity;

import org.apache.spark.Partitioner;

class CustomPartitioner extends Partitioner{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int numParts;
	private int median;
	
	public CustomPartitioner(int i) {
		numParts=i;
		//this.median = median;
	}

	@Override
	public int numPartitions()
	{
	    return numParts;
	}

	@Override
	public int getPartition(Object key){
		
		//partition based on the first character of the key...you can have your logic here !!
		//System.out.println(key);
		 return Integer.valueOf(key.toString())%numParts;
	}
	
	@Override
	public boolean equals(Object obj){
		if(obj instanceof CustomPartitioner)
		{
			CustomPartitioner partitionerObject = (CustomPartitioner)obj;
			if(partitionerObject.numParts == this.numParts)
				return true;
		}
	
		return false;
	   }
}