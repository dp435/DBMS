package PhysicalOperator;

import DBMS.Tuple;
import DBMS.TupleReaderBinary;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for joining (or taking the cross product) of two
 * tables. If no join condition exists, the operator joins on everything.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class BNLJoinOperator extends Operator {

	public Expression exp;
	public Operator leftChild;
	public Operator rightChild;
	private Tuple leftTuple;
	private int bufferCount = 1;
	private Queue<Tuple> buffer;
	private int bufferSize;

	/**
	 * Constructor for BNLJoinOperator.
	 * 
	 * @param exp
	 *            The join condition. If the expression is null, then the
	 *            operator joins on everything.
	 * @param leftChild
	 *            The left child operator of the join tree.
	 * @param rightChild
	 *            The right child operator of the join tree.
	 */
	public BNLJoinOperator(Expression exp, Operator leftChild, Operator rightChild, int bufferCount) {
		this.exp = exp;
		this.leftChild = leftChild;
		this.rightChild = rightChild;
		leftTuple = leftChild.getNextTuple();

		this.bufferCount = bufferCount;
		if (leftTuple != null) {
			bufferSize = bufferCount * (TupleReaderBinary.BUFFER_SIZE / (4 * leftTuple.size()));
			buffer = new ArrayBlockingQueue<Tuple>(bufferSize);
		} else {
			bufferSize = 0;
			buffer = new ArrayBlockingQueue<Tuple>(1);
		}

		if (leftTuple != null) {
			buffer.add(leftTuple);
		}
		FillBuffer();
		leftTuple = buffer.poll();

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
				leftTuple = GetTupleFromBuffer();
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
	 * Fills the in-memory buffer will tuples from the
	 * left child.
	 * @return true if filled, false if no more tuples to fetch
	 */
	private void FillBuffer() {
		Tuple temp;
		while (buffer.size() < bufferSize) {
			temp = leftChild.getNextTuple();
			if (temp != null) {
				buffer.add(temp);
			} else {
				return;
			}
		}
	}

	/**
	 * Returns the next tuple from the in-
	 * memory buffer.
	 * @return a tuple from the buffer
	 */
	private Tuple GetTupleFromBuffer() {
		if (buffer.size() > 0) {
			return buffer.poll();
		} else {
			FillBuffer();
			if (buffer.size() > 0) {
				return buffer.poll();
			} else {
				return null;
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
		buffer.clear();
		buffer.add(leftTuple);
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
	public void accept(PhysicalPlanVisitor visitor) {
		visitor.visit(this);
	}

}
