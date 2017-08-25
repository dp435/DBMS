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
 * Visitor to check if expression only contains equality expressions.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class CheckEqualityVisitor implements ExpressionVisitor {
	private boolean onlyEqualityExp;
	private CheckEqualityVisitor CEV;

	/**
	 * Constructor for CheckEqualityVisitor.
	 */
	public CheckEqualityVisitor() {
		onlyEqualityExp = true;
		CEV = this;
	}

	/**
	 * Method to get the result of evaluation of the expression tree when
	 * visitor is done.
	 * 
	 * @return a boolean representing the result from evaluating the expression
	 *         tree.
	 */
	public boolean getResult() {
		return onlyEqualityExp;
	}

	/**
	 * Visit method for AndExpression node based on postorder tree traversal.
	 * 
	 * This method first recursively visits its left and right expression
	 * subtree respectively.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(CEV);
		arg.getRightExpression().accept(CEV);
	}

	/**
	 * Visit method for GreaterThan node based on postorder tree traversal.
	 * 
	 * This method sets onlyEqualityExp to false.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThan arg) {
		onlyEqualityExp = false;
	}

	/**
	 * Visit method for GreaterThanEqual node based on postorder tree traversal.
	 * 
	 * This method sets onlyEqualityExp to false.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThanEquals arg) {
		onlyEqualityExp = false;
	}

	/**
	 * Visit method for MinorThan node based on postorder tree traversal.
	 * 
	 * This method sets onlyEqualityExp to false.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThan arg) {
		onlyEqualityExp = false;
	}

	/**
	 * Visit method for MinorThanEqual node based on postorder tree traversal.
	 * 
	 * This method sets onlyEqualityExp to false.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThanEquals arg) {
		onlyEqualityExp = false;
	}

	/**
	 * Visit method for EqualsTo node based on postorder tree traversal.
	 * 
	* This method does not do anything to onlyEqualityExp.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(EqualsTo arg) {
		return;
	}

	/**
	 * Visit method for NotEqualsTo node based on postorder tree traversal.
	 * 
	 * This method sets onlyEqualityExp to false.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(NotEqualsTo arg) {
		onlyEqualityExp = false;
	}

	// ********** UNSUPPORTED OPERATIONS BELOW: DO NOT SUPPORT! **********

	@Override
	public void visit(LongValue arg) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(Column arg) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

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
