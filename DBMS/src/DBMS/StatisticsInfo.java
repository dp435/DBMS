package DBMS;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

/**
 * This class is the data structure responsible for storing statistcs info for a
 * relation.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */

public class StatisticsInfo {

	Hashtable<String, Integer[]> attributeStatsTable;
	String relation;
	List<String> attributesList;
	int count;

	private static final int min_idx = 0;
	private static final int max_idx = 1;

	/**
	 * Constructor for StatisticsInfo.
	 * 
	 * @param relation
	 *            the name of the base relation.
	 */
	public StatisticsInfo(String relation) {
		attributeStatsTable = new Hashtable<String, Integer[]>();
		attributesList = DatabaseCatalog.getInstance().getAttributeInfo(relation);

		for (String attribute : attributesList) {
			attributeStatsTable.put(attribute, new Integer[] { null, null });
		}
		this.relation = relation;

		this.count = 0;
	}

	/**
	 * Method to update the min, max, and count info of a relation.
	 * 
	 * @param tuple
	 *            a tuple from the base relation.
	 */
	public void updateStats(Tuple tuple) {
		for (int i = 0; i < attributesList.size(); i++) {
			Integer value = Integer.parseInt(tuple.getField((tuple.ordering.get(i))));

			Integer current_min = attributeStatsTable.get(attributesList.get(i))[min_idx];
			Integer current_max = attributeStatsTable.get(attributesList.get(i))[max_idx];

			if (current_min == null || current_min > value)
				attributeStatsTable.get(attributesList.get(i))[min_idx] = value;

			if (current_max == null || current_max < value)
				attributeStatsTable.get(attributesList.get(i))[max_idx] = value;
		}
		count++;
	}

	/**
	 * Method to get the minimum value of an attribute.
	 * 
	 * @param attribute
	 *            the name of the attribute.
	 */
	public Integer getAttributeMinimum(String attribute) {
		return attributeStatsTable.get(attribute)[min_idx];
	}

	/**
	 * Method to get the maximum value of an attribute.
	 * 
	 * @param attribute
	 *            the name of the attribute.
	 */
	public Integer getAttributeMaximum(String attribute) {
		return attributeStatsTable.get(attribute)[max_idx];
	}

	/**
	 * Method to get the number of tuples in a relation.
	 */
	public Integer getCount() {
		return count;
	}

}
