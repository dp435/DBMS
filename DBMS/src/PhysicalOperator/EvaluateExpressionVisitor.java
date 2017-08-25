package PhysicalOperator;


import java.util.Stack;

import DBMS.Tuple;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * Visitor to evaluate a query's "where clause" expression to a single boolean.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class EvaluateExpressionVisitor implements ExpressionVisitor {

	private Stack<Boolean> booleanStack; // boolean stack generated from
											// postorder tree traversal
	private Stack<LongValue> numberStack; // value stack (of leaf nodes)
											// generated from postorder tree
											// traversal
	private Tuple tuple; // current tuple to have the expression evaluated on.
	private EvaluateExpressionVisitor EV;

	/**
	 * Constructor for EvaluateExpressionVisitor.
	 * 
	 * @param currentTuple
	 *            current tuple to have the expression evaluated on.
	 */
	public EvaluateExpressionVisitor(Tuple currentTuple) {
		booleanStack = new Stack<Boolean>();
		numberStack = new Stack<LongValue>();
		tuple = currentTuple;
		EV = this;
	}

	/**
	 * Method to get the result of evaluation of the expression tree when
	 * visitor is done.
	 * 
	 * @return a boolean representing the result from evaluating the expression
	 *         tree.
	 */
	public boolean getResult() {
		return booleanStack.pop();
	}

	/**
	 * One of the visit method for leaf node.
	 * 
	 * This method just pushes the numeric value of the node to the numberStack.
	 * 
	 * @param arg
	 *            the node of type LongValue to be visited
	 */
	@Override
	public void visit(LongValue arg) {
		numberStack.push(arg);
	}

	/**
	 * One of the visit method for leaf node.
	 * 
	 * This method looks up the the value referenced by the column and pushes
	 * its numeric value to the numberStack.
	 * 
	 * @param arg
	 *            the node of type Column to be visited
	 */
	@Override
	public void visit(Column arg) {
		String columnRef = arg.toString();
		numberStack.push(new LongValue(tuple.getField(columnRef)));
	}

	/**
	 * Visit method for AndExpression node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left and right
	 * subtree are then AND'ed together and then, pushed back to the
	 * booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		booleanStack.push(booleanStack.pop() && booleanStack.pop());
	}

	/**
	 * Visit method for GreaterThan node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the ">" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThan arg) {
		Expression leftExp = arg.getLeftExpression();
		leftExp.accept(EV);
		Expression rightExp = arg.getRightExpression();
		rightExp.accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() > rightChild.getValue()) ? true : false);
	}

	/**
	 * Visit method for GreaterThanEqual node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the ">=" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThanEquals arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() >= rightChild.getValue()) ? true : false);
	}

	/**
	 * Visit method for MinorThan node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the "<" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThan arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() < rightChild.getValue()) ? true : false);
	}

	/**
	 * Visit method for MinorThanEqual node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the "<=" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThanEquals arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() <= rightChild.getValue()) ? true : false);
	}

	/**
	 * Visit method for EqualsTo node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the "==" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(EqualsTo arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() == rightChild.getValue()) ? true : false);
	}

	/**
	 * Visit method for NotEqualsTo node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively. The result from evaluating the left subtree is
	 * compared to that of the right subtree using the "!=" operator. The result
	 * is pushed back to the booleanStack.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(NotEqualsTo arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
		LongValue rightChild = numberStack.pop();
		LongValue leftChild = numberStack.pop();
		booleanStack.push((leftChild.getValue() != rightChild.getValue()) ? true : false);
	}

	// ********** UNSUPPORTED OPERATIONS BELOW: DO NOT SUPPORT! **********

	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(DateValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(TimeValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(TimestampValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Parenthesis arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(StringValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Addition arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Division arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Multiplication arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Subtraction arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(OrExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Between arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(InExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(IsNullExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(LikeExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Function arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(InverseExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(JdbcParameter arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(DoubleValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(SubSelect arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(CaseExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(WhenClause arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(ExistsExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Concat arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Matches arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(BitwiseOr arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(BitwiseXor arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

}
