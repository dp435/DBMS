package DBMS;

import java.util.Comparator;

/**
 * This class is responsible for overriding the compare
 * method for comparing two tuples.  To be used in a 
 * Collections.sort call for sorting a list of tuples
 * based on the key priority given in the comparisonFields
 * String array.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class RecordComparator implements Comparator<Tuple> {

	String[] comparisonFields;

	/**
	 * Initializes a RecordComparator with a String array
	 * of comparisonFields in the order two tuples are
	 * to be compared.
	 * @param comparisonFields
	 */
	public RecordComparator(String[] comparisonFields) {
		this.comparisonFields = comparisonFields;
	}

	/**
	 * @param o1 The first tuple to compare
	 * @param o2 The second tuple to compare
	 * @return 1 if o1 comes after o2 in
	 * ascending order when ordered by the keys
	 * listed in comparisonFields; -1 if o2 comes
	 * before o1; 0 if the tuples are equal.
	 */
	@Override
	public int compare(Tuple o1, Tuple o2) {
		for (int i = 0; i < comparisonFields.length; i++) {
			if (o1 == null && o2 == null){
				return 0;
			}			
			if (o1 == null && o2 != null)
				return 1;
			
			if (o2 == null && o1 != null)
				return -1;
			
			if (Integer.parseInt(o1.getField(comparisonFields[i])) > Integer.parseInt(o2.getField(comparisonFields[i]))) {
				return 1;
			}
			if (Integer.parseInt(o1.getField(comparisonFields[i])) < Integer.parseInt(o2.getField(comparisonFields[i]))) {
				return -1;
			}
		}
		return 0;
	}

}