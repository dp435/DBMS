package PhysicalOperator;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import DBMS.Interpreter;
import DBMS.Tuple;
import DBMS.TupleCompare;
import LogicalOperator.EvaluateJoinVisitor;
import net.sf.jsqlparser.expression.Expression;

/**
 * This class is responsible for joining tables using the sort-merge-join
 * algorithm. This algorithm can only handle equijoins.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class SMJOperator extends Operator {

	public ArrayList<Expression> conditionList;
	public Operator outerChild;
	public Operator innerChild;
	private boolean isInitialized;

	private Tuple Tr;
	private Tuple Ts;
	private Tuple Gs;

	private int innerTupleSize;

	private TupleCompare comparator;
	private boolean outerReset;
	private boolean innerReset;

	private int Ts_idx;
	private int Gs_idx;

	public static final int BUFFER_SIZE = 4096;
	public static final int METADATA_SIZE = 8;
	public static final int BYTE_SIZE = 4;

	/**
	 * Constructor for SMJOperator.
	 * 
	 * @param exp
	 *            The join condition. Must be an equality condition
	 * @param outerChild
	 *            The left child operator of the join tree.
	 * @param innerChild
	 *            The right child operator of the join tree.
	 */
	public SMJOperator(Expression exp, Operator outerChild, Operator innerChild) {
		EvaluateJoinVisitor JV = new EvaluateJoinVisitor();

		exp.accept(JV);
		conditionList = JV.getJoinList();
		this.outerChild = outerChild;
		this.innerChild = innerChild;
		isInitialized = false;

		comparator = new TupleCompare();

		Tr = null;
		Ts = null;
		Gs = null;
		outerReset = false;
		innerReset = false;

		Ts_idx = 0;
		Gs_idx = 0;

		innerTupleSize = 0;
	}

	/**
	 * Method to get the next joined tuple satisfying the join condition if such
	 * exists.
	 * 
	 * @return joined tuple satisfying the join condition.
	 */
	@Override
	public Tuple getNextTuple() {

		// Initialization: run once during entire lifetime.
		if (!isInitialized) {
			Tr = outerChild.getNextTuple(); // outer table pointer
			Ts = innerChild.getNextTuple(); // inner table pointer
			Gs = Ts; // inner partition pointer

			if (Ts != null)
				innerTupleSize = Ts.size();

			// SAVE PARTITION INFO
			Ts_idx++;
			Gs_idx++;

			isInitialized = true;
		}

		while (Tr != null && Gs != null) {

			// conditional check to ensure only 1 reset per while-loop iteration
			if (!outerReset) {
				// Step 1: find inner partition that satisfies on outerTuple.
				while (comparator.lessThan(Tr, Gs, conditionList)) {
					Tr = outerChild.getNextTuple();
				}

				while (comparator.greaterThan(Tr, Gs, conditionList)) {
					Gs = innerChild.getNextTuple();
					// MOVE PARTITION POINTER
					Gs_idx++;
				}

				// Step 2: move inner pointer to start of current partition.

				// RESET INNER POINTER
				Ts = Gs;
				Ts_idx = Gs_idx;
				// RESET PARTITION
				innerChild.reset(Ts_idx);

				outerReset = true;
			}

			// Step 3: Find satisfying outer and inner tuples.
			while (Tr != null && comparator.equals(Tr, Gs, conditionList)) {

				// conditional check to ensure only 1 reset per while-loop
				// iteration
				if (!innerReset) {
					Ts = Gs;
					innerReset = true;

					// RESET INNER POINTER
					Ts_idx = Gs_idx;
					// RESET PARTITION
					innerChild.reset(Ts_idx);
				}

				// Step 4: begin joining tuples
				while (Ts != null && comparator.equals(Ts, Tr, conditionList)) {
					List<String> ordering = new ArrayList<>();
					ordering.addAll(Tr.ordering);
					ordering.addAll(Ts.ordering);
					Tuple result = new Tuple("SMJTuple", new Hashtable<String, String>(), ordering);
					result.getHashtable().putAll(Tr.getHashtable());
					result.getHashtable().putAll(Ts.getHashtable());

					Ts = innerChild.getNextTuple();

					// MOVE INNER TABLE POINTER
					Ts_idx++;

					return result;
				}

				// Step 5: move on to next outer tuple
				Tr = outerChild.getNextTuple();
				innerReset = false;
			}
			Gs = Ts;

			// UPDATE PARTITION POINTER
			Gs_idx = Ts_idx;

			outerReset = false;
		}
		return null;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		outerChild.reset();
		innerChild.reset();
		isInitialized = false;
		Tr = null;
		Ts = null;
		Gs = null;
		outerReset = false;
		innerReset = false;
		innerTupleSize = 0;
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
