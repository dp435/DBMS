package PhysicalOperator;

import LogicalOperator.LogicalPlanVisitor;

public abstract class OperatorNode {
		/**
		 * Abstract method for accepting visitor
		 * 
		 * @param visitor
		 *            visitor to be accepted
		 */
		public abstract void accept(LogicalPlanVisitor visitor);
	
}
