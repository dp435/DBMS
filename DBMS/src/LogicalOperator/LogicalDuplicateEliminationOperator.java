package LogicalOperator;

public class LogicalDuplicateEliminationOperator extends LogicalOperator {

	public LogicalOperator childOperator;

	/**
	 * Constructor for LogicalDuplicateEliminationOperator.
	 * 
	 * @param sortOperator
	 *            The topmost (i.e. current root) operator node in the operator
	 *            tree. This node must be a LogicalSortOperator for
	 *            DuplicateEliminationOperator to work properly.
	 */
	public LogicalDuplicateEliminationOperator(LogicalOperator sortOperator) {
		childOperator = sortOperator;
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
