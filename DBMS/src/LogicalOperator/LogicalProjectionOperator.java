package LogicalOperator;

import java.util.List;

/**
 * Logical projection operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalProjectionOperator extends LogicalOperator {

	public List projectionCondition;
	public LogicalOperator childOperator;
	
	/**
	 * Constructor for ProjectionOperator.
	 * 
	 * @param projectionCondition
	 *            Columns to be projected on.
	 */
	public LogicalProjectionOperator(List projectionCondition, LogicalOperator child) {
		this.projectionCondition = projectionCondition;
		childOperator = child;
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
