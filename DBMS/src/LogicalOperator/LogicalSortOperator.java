package LogicalOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * Logical sort operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */
public class LogicalSortOperator extends LogicalOperator {

	public LogicalOperator childOperator;
	public List<String> outputOrderList;
	public ArrayList<String> sortOrder;
	public List<String> printOrder;
	
	private String sortMethod;
	private int bufferCount;

	/**
	 * Constructor for LogicalSortOperator.
	 * 
	 * @param child
	 *            The topmost (i.e. current root) operator node in the operator
	 *            tree.
	 * @param orderByList
	 *            The sequence of columns to order by.
	 * @param outputOrder
	 *            The sequence representing the field names of the resulting
	 *            tuple from left-to-right.
	 * @param sortMethod
	 *            The sort method to be used.
	 * @param bufferCount
	 *            The number of buffers to be alloted to the sort operator.
	 */
	public LogicalSortOperator(LogicalOperator child, List orderByList, List<String> outputOrder, String sortMethod,
			int bufferCount, List<String> printOrder) {
		childOperator = child;
		if (outputOrder != null) {
			outputOrderList = new ArrayList<String>(outputOrder);
			sortOrder = new ArrayList<String>();
			this.printOrder = printOrder;
			for (Object item : orderByList) {
				sortOrder.add(item.toString());
				outputOrderList.remove(item.toString());
			}
			sortOrder.addAll(outputOrderList);
		} else
			sortOrder = new ArrayList<String>(orderByList);
		this.sortMethod = sortMethod;
		this.bufferCount = bufferCount;
	}

	/**
	 * Getter for sortMethod.
	 * 
	 * @return The sort method to be used.
	 */
	public String getSortMethod() {
		return sortMethod;
	}

	/**
	 * Getter for bufferCount.
	 * 
	 * @return The buffer count to be alloted to the sort operator.
	 */
	public int getBufferCount() {
		return bufferCount;
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
