package LogicalOperator;

/**
 * Logical scan operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalScanOperator extends LogicalOperator {

	public String tableName;
	public String aliasName;

	/**
	 * Constructor for ScanOperator.
	 * 
	 * @param tableName
	 *            The name of table to be scanned.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 */
	public LogicalScanOperator(String tableName, String aliasName) {
		this.tableName = tableName;
		this.aliasName = aliasName;
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
