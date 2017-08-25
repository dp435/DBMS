package PhysicalOperator;

import DBMS.Tuple;

public abstract class Operator {

	/**
	 * Called repeatedly to get the next tuple of the operator’s output.
	 */
	public abstract Tuple getNextTuple();

	/**
	 * Tells the operator to reset its state and start returning its output
	 * again from the beginning.
	 */
	public abstract void reset();

	/**
	 * Tells the operator to reset its state and start returning its output from
	 * the specified index.
	 */
	public abstract void reset(int index);
	
	/**
	 * Abstract method for accepting visitor
	 * 
	 * @param visitor
	 *            visitor to be accepted
	 */
	public abstract void accept(PhysicalPlanVisitor visitor);

}
