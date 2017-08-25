package PhysicalOperator;

import java.util.ArrayList;
import java.util.List;

import DBMS.RecordComparator;
import DBMS.Tuple;

/**
 * This class is responsible for sorting the resulting tuples.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class SortOperator extends Operator {

	public Operator childOperator;
	private List<String> outputOrderList;
	private List<String> sortOrder;
	private List<Tuple> resultAccumulator;
	private int resultIdx;
	public List<String> printOrder;

	/**
	 * Constructor for SortOperator.
	 * 
	 * @param child
	 *            The topmost (i.e. current root) operator node in the operator
	 *            tree.
	 * @param orderByList
	 *            The sequence of columns to order by.
	 * @param outputOrder
	 *            The sequence representing the field names of the resulting
	 *            tuple from left-to-right.
	 */
	public SortOperator(Operator child, List orderByList, List<String> outputOrder, List<String> printOrder) {
		this.printOrder = printOrder;
		childOperator = child;
		if (outputOrder != null) {
			outputOrderList = new ArrayList<String>(outputOrder);
			sortOrder = new ArrayList<String>();
			for (Object item : orderByList) {
				sortOrder.add(item.toString());
				outputOrderList.remove(item.toString());
			}
			sortOrder.addAll(outputOrderList);
		} else
			sortOrder = new ArrayList<String>(orderByList);
		resultAccumulator = null;
		resultIdx = 0;
	}

	/**
	 * Method to get the next tuple after result is sorted.
	 * 
	 * @return next tuple after being sorted
	 */
	@Override
	public Tuple getNextTuple() {
		if (resultAccumulator == null) {
			resultAccumulator = new ArrayList<Tuple>();
			Tuple currentTuple = null;
			while ((currentTuple = childOperator.getNextTuple()) != null) {
				resultAccumulator.add(currentTuple);
			}
			String[] comparisons = sortOrder.toArray(new String[0]);
			RecordComparator sorter = new RecordComparator(comparisons);
			resultAccumulator.sort(sorter);
		}

		if (resultIdx < resultAccumulator.size()) {
			Tuple currentTuple = resultAccumulator.get(resultIdx);
			resultIdx++;
			return currentTuple;
		} else
			return null;
	}

	/**
	 * Method to reset the state of the operator to start returning its output
	 * again from the beginning.
	 */
	@Override
	public void reset() {
		childOperator.reset();
		resultAccumulator = null;
		resultIdx = 0;
	}

	/**
	 * Method to reset the state of the operator to specified index.
	 */
	public void reset(int index) {
		resultIdx = index;
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
