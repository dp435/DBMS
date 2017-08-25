package DBMS;
import java.util.ArrayList;
import java.util.List;

/**
 * This class holds information about the fields and their respective
 * values for a particular tuple.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class FieldOrderedTuple {
	List<Integer> tuples;

	/**
	 * Initializes a FieldOrderedTuple with a list of tuple values
	 * @param tuples
	 */
	public FieldOrderedTuple(List<Integer> tuples) {
		this.tuples = tuples;
	}
	
	/**
	 * Initializes a FieldOrderedTuple with an empty list of tuple values
	 */
	public FieldOrderedTuple() {
		tuples = new ArrayList<>();
	}

	/**
	 * Sets the int value for the given index.
	 * @param index
	 * @param value
	 */
	public void setField(int index, int value) {
		tuples.set(index, value);
	}
	
	/**
	 * Adds the int value to the end of the list.
	 * @param index
	 * @param value
	 */
	public void addField(int value) {
		tuples.add(value);
	}

	/**
	 * Returns the int value at the given index.
	 * @param index
	 * @return the int stored in at the given index
	 */
	public int getField(int index) {
		return tuples.get(index);
	}

	/** The overridden toString method for this class.
	 * @return A string describing the enumeration of this the tuples list
	 */
	public String toString() {
		String builder = "";
		for (int i = 0; i < tuples.size(); i++){
			builder += tuples.get(i) + " ";
		}
		return builder;
	}
	
	/**
	 * A function to return the size of the tuples list.
	 * @return the number of elements in the tuples list.
	 */
	public int size(){
		return tuples.size();
	}
	
	@Override
	public boolean equals(Object other){
		if (other == null){
			return false;
		}
		if (!(other instanceof FieldOrderedTuple)){
			return false;
		}
		
		if (other == this){
			return true;
		}
		
		return tuples.equals(((FieldOrderedTuple)other).tuples);
		
	}
}
