package PhysicalOperator;

import DBMS.Tuple;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for joining (or taking the cross product) of two
 * tables. If no join condition exists, the operator joins on everything.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class TNLJoinOperator extends Operator {

	public Expression exp;
	public Operator leftChild;
	public Operator rightChild;
	private Tuple leftTuple;

	/**
	 * Constructor for JoinOperator.
	 * 
	 * @param exp
	 *            The join condition. If the expression is null, then the
	 *            operator joins on everything.
	 * @param leftChild
	 *            The left child operator of the join tree.
	 * @param rightChild
	 *            The right child operator of the join tree.
	 */
	public TNLJoinOperator(Expression exp, Operator leftChild, Operator rightChild) {
		this.exp = exp;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		leftTuple = leftChild.getNextTuple();

	}

	/**
	 * Method to get the next joined tuple satisfying the join condition if such
	 * exists.
	 * 
	 * @return joined tuple satisfying the join condition.
	 */
	@Override
	public Tuple getNextTuple() {

		Tuple rightTuple = null;
		Tuple joinedTuple = null;

		while (true) {
			if (leftTuple == null) {
				return null;
			}
			rightTuple = rightChild.getNextTuple();
			if (rightTuple == null) {
				rightChild.reset();
				leftTuple = leftChild.getNextTuple();
				continue;
			}
			List<String> ordering = new ArrayList<>();
			ordering.addAll(leftTuple.ordering);
			ordering.addAll(rightTuple.ordering);
			joinedTuple = new Tuple("JoinedTuple", new Hashtable<String, String>(), ordering);
			joinedTuple.getHashtable().putAll(leftTuple.getHashtable());
			joinedTuple.getHashtable().putAll(rightTuple.getHashtable());
			if (exp == null) {
				return joinedTuple;
			} else {
				EvaluateExpressionVisitor EV = new EvaluateExpressionVisitor(joinedTuple);
				exp.accept(EV);
				if (EV.getResult() == true) {
					return joinedTuple;
				}
			}
		}
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		leftChild.reset();
		rightChild.reset();
		leftTuple = leftChild.getNextTuple();
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
