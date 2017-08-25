package LogicalOperator;

/**
 * Top-level abstract class for a logical operator in query plan tree.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */
public abstract class LogicalOperator {

	/**
	 * Abstract method for accepting visitor
	 * 
	 * @param visitor
	 *            visitor to be accepted
	 */
	public abstract void accept(LogicalPlanVisitor visitor);
}
