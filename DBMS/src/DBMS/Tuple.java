package DBMS;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

/**
 * This class holds information about the fields and their respective
 * values for a particular tuple.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class Tuple {
	String tableName;
	Hashtable<String, String> tuples;
	public List<String> ordering;

	/**
	 * Initializes a Tuple with for given tablename with
	 * a given Hashtable of field/value pairs.
	 * @param tableName
	 * @param tuples
	 */
	public Tuple(String tableName, Hashtable<String, String> tuples, List<String> ordering) {
		this.tableName = tableName;
		this.tuples = tuples;
		this.ordering = ordering;
	}

	/**
	 * Sets the String value for the given field.
	 * @param fieldName
	 * @param value
	 */
	public void setField(String fieldName, String value) {
		tuples.put(fieldName, value);
	}

	/**
	 * Returns the String value in the given field.
	 * @param fieldName
	 * @return the String stored in the given field
	 */
	public String getField(String fieldName) {
		return tuples.get(fieldName);
	}

	/**
	 * Returns the internal hashtable containing the field/value pairs.
	 * @return the Hashtable containing the values for each field (key)
	 */
	public Hashtable<String, String> getHashtable() {
		return tuples;
	}

	/**
	 * @return A string describing this tuple and its field/value pairs
	 */
	public String toString() {
		Enumeration<String> keys = tuples.keys();
		String builder = "";
		while (keys.hasMoreElements()) {
			String nextElement = keys.nextElement();
			builder += nextElement + " : " + tuples.get(nextElement) + "\n";
		}
		return builder;
	}
	
	public int size(){
		return tuples.size();
	}
	
	public FieldOrderedTuple convertToFOT(){
		FieldOrderedTuple ret = new FieldOrderedTuple();
		for (int i = 0; i < ordering.size(); i++){
			ret.addField(Integer.parseInt(tuples.get(ordering.get(i))));
		}	
		return ret;
	}
	
	public void addOrdering(String fieldname){
		ordering.add(fieldname);
	}

	/**
	 * Returns whether this tuple is equal to another
	 * given tuple.
	 * @param o
	 * @return True if this tuple is equal to o, false
	 * otherwise.
	 */
	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}

		if (!(o instanceof Tuple)) {
			return false;
		}

		return this.tuples.equals(((Tuple) o).getHashtable());
	}
}
