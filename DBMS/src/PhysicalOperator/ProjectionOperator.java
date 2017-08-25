package PhysicalOperator;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import DBMS.Tuple;

/**
 * This class is responsible for projecting columns of the tuple specified by
 * the query.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class ProjectionOperator extends Operator {

	public Operator child;

	public List selectClause;

	/**
	 * Constructor for ProjectionOperator.
	 * 
	 * @param selectClause
	 *            The select clause of the query. This indicates which columns
	 *            to project on.
	 * @param child
	 *            The child operator whose primary job is to get the next tuple
	 *            to be projected on. The child can either be a ScanOperator or
	 *            a SelectionOperator.
	 */
	public ProjectionOperator(List selectClause, Operator child) {
		this.selectClause = selectClause;
		this.child = child;
	}

	/**
	 * Method to get the next tuple after being projected on.
	 * 
	 * @return tuple after having its columns projected on.
	 */
	@Override
	public Tuple getNextTuple() {
		Tuple currentTuple = null;
		if ((currentTuple = child.getNextTuple()) != null) {
			Tuple extractedTuple = new Tuple("ProjectedTuple", new Hashtable<String, String>(),
					new ArrayList<String>());
			for (Object fieldname : selectClause) {
				extractedTuple.setField(fieldname.toString(), currentTuple.getField(fieldname.toString()));
				extractedTuple.addOrdering(fieldname.toString());
			}
			return extractedTuple;
		}
		return null;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		child.reset();
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
