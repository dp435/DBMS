package LogicalOperator;

/**
 * Logical index scan operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */


public class LogicalIndexScanOperator extends LogicalOperator {
	public String tableName;
	public String aliasName;
	public Integer lowerBound;
	public Integer upperBound;
	public String indexedAttribute;
	public boolean isClustered;

	/**
	 * Constructor for IndexScanOperator.
	 * 
	 * @param tableName
	 *            The name of table to be scanned.
	 * @param aliasName
	 *            The alias name for the current table. If none exists, then it
	 *            is set to null.
	 */
	public LogicalIndexScanOperator(String tableName, String aliasName, String indexedAttribute, boolean isClustered, Integer lowerBound, Integer upperBound) {
		this.tableName = tableName;
		this.aliasName = aliasName;
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		this.indexedAttribute = indexedAttribute;
		this.isClustered = isClustered;
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
