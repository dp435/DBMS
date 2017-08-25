package DBMS;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for testing the equality, less than, and greater
 * than relations between 2 tuples.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class TupleCompare {

	/**
	 * Method to compare if lessThanTuple is less than greaterThanTuple.
	 * 
	 * @param lessThanTuple
	 *            The tuple expected to be smaller
	 * @param greaterThanTuple
	 *            The tuple expected to be larger
	 * @param expressionList
	 *            Expression involving fields of the two input tuples
	 * @return true if lessThanTuple is less than greaterThanTuple; false,
	 *         otherwise.
	 */
	public boolean lessThan(Tuple lessThanTuple, Tuple greaterThanTuple, ArrayList<Expression> expressionList) {
		if (greaterThanTuple == null || lessThanTuple == null)
			return false;
		for (Expression exp : expressionList) {
			String[] condition = exp.toString().split("=");
			String leftCol = condition[0].trim();
			String rightCol = condition[1].trim();

			int lessThanValue = -1;
			int greaterThanValue = -1;

			try {
				lessThanValue = Integer.parseInt(lessThanTuple.getField(leftCol));
				greaterThanValue = Integer.parseInt(greaterThanTuple.getField(rightCol));
			} catch (Exception e) {
				lessThanValue = Integer.parseInt(lessThanTuple.getField(rightCol));
				greaterThanValue = Integer.parseInt(greaterThanTuple.getField(leftCol));
			}

			if (lessThanValue != greaterThanValue) {

				if (lessThanValue < greaterThanValue)
					return true;
				else if (lessThanValue > greaterThanValue)
					return false;
			}
		}
		return false;
	}

	/**
	 * Method to compare if greaterThanTuple is greater than lessThanTuple.
	 * 
	 * @param lessThanTuple
	 *            The tuple expected to be smaller
	 * @param greaterThanTuple
	 *            The tuple expected to be larger
	 * @param expressionList
	 *            Expression involving fields of the two input tuples
	 * @return true if greaterThanTuple is greater than lessThanTuple; false,
	 *         otherwise.
	 */
	public boolean greaterThan(Tuple greaterThanTuple, Tuple lessThanTuple, ArrayList<Expression> expressionList) {
		if (greaterThanTuple == null || lessThanTuple == null)
			return false;
		for (Expression exp : expressionList) {
			String[] condition = exp.toString().split("=");
			String leftCol = condition[0].trim();
			String rightCol = condition[1].trim();

			int greaterThanValue = -1;
			int lessThanValue = -1;

			try {
				greaterThanValue = Integer.parseInt(greaterThanTuple.getField(leftCol));
				lessThanValue = Integer.parseInt(lessThanTuple.getField(rightCol));
			} catch (Exception e) {
				greaterThanValue = Integer.parseInt(greaterThanTuple.getField(rightCol));
				lessThanValue = Integer.parseInt(lessThanTuple.getField(leftCol));
			}

			if (greaterThanValue != lessThanValue) {
				if (greaterThanValue > lessThanValue)
					return true;
				else if (greaterThanValue < lessThanValue)
					return false;
			}
		}
		return false;
	}

	/**
	 * Method to compare if left tuple is equal to right tuple.
	 * 
	 * @param leftTuple
	 *            The left tuple
	 * @param rightTuple
	 *            The right tuple
	 * @param expressionList
	 *            Expression involving fields of the two input tuples
	 * @return true if leftTuple is equal to rightTuple in fields that show up
	 *         in the expressionList
	 */
	public boolean equals(Tuple leftTuple, Tuple rightTuple, ArrayList<Expression> expressionList) {
		if (leftTuple == null || rightTuple == null)
			return false;
		for (Expression exp : expressionList) {
			String[] condition = exp.toString().split("=");
			String leftCol = condition[0].trim();
			String rightCol = condition[1].trim();

			int leftValue = -1;
			int rightValue = -1;

			try {
				leftValue = Integer.parseInt(leftTuple.getField(leftCol));
				rightValue = Integer.parseInt(rightTuple.getField(rightCol));
			} catch (Exception e) {
				leftValue = Integer.parseInt(leftTuple.getField(rightCol));
				rightValue = Integer.parseInt(rightTuple.getField(leftCol));
			}

			if (leftValue != rightValue) {
				return false;
			}
		}
		return true;
	}

}
