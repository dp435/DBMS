package DBMS;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class TupleWriterHuman {
	PrintWriter writer;
	public TupleWriterHuman(String outputLocation) throws FileNotFoundException{
			writer = new PrintWriter(outputLocation);
	}
	
	public void writeTuple(FieldOrderedTuple tuple){
		String resultAccumulator = "";
		for (Integer value : tuple.tuples) {
			resultAccumulator += value.toString() + ",";
		}
		if (!resultAccumulator.equals("")) 
			resultAccumulator = resultAccumulator.substring(0, resultAccumulator.lastIndexOf(","));
		writer.println(resultAccumulator);
	}
	
	public void close(){
		writer.close();
	}
}
