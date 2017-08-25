package LogicalOperator;

import java.util.List;

import DBMS.DatabaseCatalog;
import PhysicalOperator.DJoinOperator;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * Custom data structure to hold the LogicalOperator and other relevant info.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalJoinChild implements DJoinOperator {

	private String relationName;
	private String aliasName;
	private LogicalOperator operator;
	private DatabaseCatalog catalog;
	private Expression joinExpression;

	/**
	 * Constructor for LogicalJoinChild
	 * 
	 * @param relationName
	 *            the base relation name.
	 * @param aliasName
	 *            the alias name; null if none exists.
	 * @param childOperator
	 *            the logical operator for the child of the join operator.
	 */
	public LogicalJoinChild(String relationName, String aliasName, LogicalOperator childOperator) {
		this.relationName = relationName;
		this.aliasName = aliasName;
		this.operator = childOperator;
		catalog = DatabaseCatalog.getInstance();
	}

	/**
	 * Method to get the child operator.
	 * 
	 * @return the child operator.
	 */
	public LogicalOperator getOperator() {
		return operator;
	}

	/**
	 * Method to get the base relation name.
	 * 
	 * @return the base relation name.
	 */
	public String getRelationName() {
		return relationName;
	}

	/**
	 * Method to get the alias name.
	 * 
	 * @return the alias name; null if none exists.
	 */
	public String getAliasName() {
		return aliasName;
	}

	/**
	 * Method to return the alias or relation name.
	 * 
	 * @return alias name if aliasName not null; else, relation name.
	 */
	public String getARName() {
		if (aliasName != null && aliasName != "") {
			return aliasName;
		} else {
			return relationName;
		}
	}

	/**
	 * Method to get the max value of an attribute of current relation.
	 * 
	 * @return the max value of the attribute.
	 */
	public int getMax(String attribute) {
		return catalog.getStatistics(relationName).getAttributeMaximum(attribute);
	}

	/**
	 * Method to get the min value of an attribute of current relation.
	 * 
	 * @return the min value of the attribute.
	 */
	public int getMin(String attribute) {
		return catalog.getStatistics(relationName).getAttributeMinimum(attribute);
	}

	/**
	 * Method to get the number of tuples in the current relation.
	 * 
	 * @return the total number of tuples.
	 */
	public int getNumTuples() {
		return catalog.getStatistics(relationName).getCount();
	}

	/**
	 * Method to get all attributes for the current relation.
	 * 
	 * @return the attributes of the current relation.
	 */
	public List<String> getAllAttributes() {
		return catalog.getAttributeInfo(relationName);
	}

	/**
	 * Method to add a join expression to the condition accumulator.
	 * 
	 * @param exp
	 *            the expression to be added.
	 */
	public void addJoinExpression(Expression exp) {
		if (joinExpression == null) {
			joinExpression = exp;
		} else {
			joinExpression = new AndExpression(joinExpression, exp);
		}
	}
}
