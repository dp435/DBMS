package PhysicalOperator;

import DBMS.Tuple;

/**
 * This class is responsible for eliminating all duplicates. The
 * DuplicateEliminationOperator assumes that the input from its child is in
 * sorted order.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class DuplicateEliminationOperator extends Operator {

	Operator sorter;
	Tuple previousTuple;
	boolean fetchedFirstTuple;

	/**
	 * Constructor for SortOperator.
	 * 
	 * @param sortOperator
	 *            The topmost (i.e. current root) operator node in the operator
	 *            tree. This node must be a SortOperator for
	 *            DuplicateEliminationOperator to work properly.
	 */
	public DuplicateEliminationOperator(Operator sortOperator) {
		sorter = sortOperator;
		previousTuple = null;
		fetchedFirstTuple = false;
	}

	/**
	 * Method to get the next non-duplicate tuple.
	 * 
	 * @return next tuple after being sorted
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currentTuple;
		while ((currentTuple = sorter.getNextTuple()) != null) {
			if (fetchedFirstTuple == false) {
				previousTuple = currentTuple;
				fetchedFirstTuple = true;
				return currentTuple;

			} else if (!(previousTuple.getHashtable()).equals(currentTuple.getHashtable())) {
				previousTuple = currentTuple;
				return currentTuple;
			}
		}
		return null;
	}

	@Override
	public void reset() {
		sorter.reset();
		previousTuple = null;
		fetchedFirstTuple = false;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * from the specified index. If the Operator is not a sort operator, this
	 * function does nothing.
	 */
	@Override
	public void reset(int index) {
	}

	/**
	 * Method for accepting visitor; just calls back visitor. Visitor method
	 * uses postorder traversal.
	 * 
	 * @param visitor
	 *            visitor to be accepted
	 */
	@Override
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
