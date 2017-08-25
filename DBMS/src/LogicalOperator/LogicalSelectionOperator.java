package LogicalOperator;

import net.sf.jsqlparser.expression.Expression;

/**
 * Logical selection operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalSelectionOperator extends LogicalOperator {

	public String tableName;
	public String aliasName;
	public Expression selectionCondition;
	public LogicalOperator scanner;

	/**
	 * Constructor for SelectionOperator.
	 * 
	 * @param tableName
	 *            The name of table to be selected on.
	 * @param selectionCondition
	 *            The selection conditions for the current table.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 */
	public LogicalSelectionOperator(String tableName, Expression selectionCondition, String aliasName, LogicalOperator child) {
		this.tableName = tableName;
		this.selectionCondition = selectionCondition;
		this.aliasName = aliasName;
		scanner = child;
	}

	/**
	 * Method for accepting visitor; just calls back visitor. Visitor method
	 * uses postorder traversal.
	 * 
	 * @param visitor
	 *            visitor to be accepted
	 */
	public void accept(LogicalPlanVisitor visitor) {
		visitor.visit(this);
	}
}
