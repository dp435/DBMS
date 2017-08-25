package LogicalOperator;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import DBMS.UnionFind;
import PhysicalOperator.Operator;

/**
 * Logical join operator class.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalJoinOperator extends LogicalOperator {

	public Expression joinCondition;
	private List<LogicalJoinChild> childOperators;
	private String joinMethod;
	private int bufferCount;
	private UnionFind UnionFindDatabase;
	public List<Expression> residualJoins;
	
	/**
	 * Constructor for LogicalJoinOperator.
	 * 
	 * @param joinCondition
	 *            The join condition. If the expression is null, then the
	 *            operator joins on everything.
	 * @param leftOperator
	 *            The left child operator of the join tree.
	 * @param rightOperator
	 *            The right child operator of the join tree.
	 * @param joinMethod
	 *            The join method to be used.
	 * @param bufferCount
	 *            The number of join buffers to be used.
	 */
	public LogicalJoinOperator() {
		joinCondition = null;
		childOperators = new ArrayList<LogicalJoinChild>();
		UnionFindDatabase = null;
		residualJoins = null;
		
		// TODO HARDCODING FOR NOW. HAVE TO REMOVE**********************
		// WARNING: GET RID OF THIS LATER! *****************************
		this.joinMethod = "TNLJ";
		this.bufferCount = 5;
		// REMEMBER TO REMOVE THIS!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	}

	/**
	 * Getter for operator list.
	 */
	public List<LogicalJoinChild> getAllOperators() {
		return childOperators;
	}

	/**
	 * Getter for right operator.
	 * 
	 * @return The right operator.
	 */
	public void addChildOperator(LogicalJoinChild child) {
		childOperators.add(child);
	}

	public Expression getJoinCondition() {
		return joinCondition;
	}

	public void setJoinCondition(List<Expression> joinConditionList) {
		for (Expression exp : joinConditionList) {
			joinCondition = (joinCondition == null) ? exp : new AndExpression(joinCondition, exp);
		}
	}

	/**
	 * Getter for joinMethod.
	 * 
	 * @return The join method to be used.
	 */
	public String getJoinMethod() {
		return joinMethod;
	}

	/**
	 * Getter for bufferCount.
	 * 
	 * @return The buffer count to be alloted to the join operator.
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
	
	public void setUnionFindDatabase(UnionFind UFData) {
		UnionFindDatabase = UFData;
	}
	
	public UnionFind getUnionFindDatabase() {
		return UnionFindDatabase;
	}
}
