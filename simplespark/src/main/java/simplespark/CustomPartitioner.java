package simplespark;

import org.apache.spark.Partitioner;
import java.util.Random;

class CustomPartitioner extends Partitioner{

	private int numParts;
	
	public CustomPartitioner(int i) {
		numParts=i;
	}

	@Override
	public int numPartitions()
	{
	    return numParts;
	}

	@Override
	public int getPartition(Object key){
		
		//partition based on the first character of the key...you can have your logic here !!
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