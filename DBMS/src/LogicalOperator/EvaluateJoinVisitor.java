package LogicalOperator;

import java.util.ArrayList;
import java.util.Stack;

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
 * Visitor to process a query's "where clause" expression and separate
 * expressions involving selection operators from those involving join
 * operators.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class EvaluateJoinVisitor implements ExpressionVisitor {

	private ArrayList<Expression> selectionList; // List of expressions
													// involving selection
													// operator.
	private ArrayList<Expression> joinList; // List of expressions involving
											// join operator.
	private EvaluateJoinVisitor JV;

	/**
	 * Constructor for EvaluateJoinVisitor.
	 */
	public EvaluateJoinVisitor() {
		selectionList = new ArrayList<Expression>();
		joinList = new ArrayList<Expression>();
		JV = this;
	}

	/**
	 * Method to get the list of expressions involving selection operator when
	 * visitor is done.
	 * 
	 * @return an ArrayList of expressions involving selection operator.
	 */
	public ArrayList<Expression> getSelectionList() {
		return selectionList;
	}

	/**
	 * Method to get the list of expressions involving join operator when
	 * visitor is done.
	 * 
	 * @return an ArrayList of expressions involving join operator.
	 */
	public ArrayList<Expression> getJoinList() {
		return joinList;
	}

	/**
	 * Visit method for AndExpression node based on postorder tree traversal.
	 * 
	 * This method first recursively visits its left and right subtree
	 * respectively.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(JV);
		arg.getRightExpression().accept(JV);
	}

	/**
	 * Visit method for GreaterThan node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then the expression requires a SelectionOperator and thus,
	 * this expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThan arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	/**
	 * Visit method for GreaterThanEquals node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then this expression requires a SelectionOperator and thus,
	 * the expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThanEquals arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	/**
	 * Visit method for MinorThan node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then this expression requires a SelectionOperator and thus,
	 * the expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThan arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	/**
	 * Visit method for MinorThanEquals node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then this expression requires a SelectionOperator and thus,
	 * the expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThanEquals arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	/**
	 * Visit method for EqualsTo node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then this expression requires a SelectionOperator and thus,
	 * the expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(EqualsTo arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	/**
	 * Visit method for NotEqualsTo node.
	 * 
	 * This method checks the type of both its child. If either one is of type
	 * LongValue, then this expression requires a SelectionOperator and thus,
	 * the expression is added to the selectionList; else, it is added to the
	 * joinList.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(NotEqualsTo arg) {
		Expression leftExp = arg.getLeftExpression();
		Expression rightExp = arg.getRightExpression();
		if (leftExp instanceof LongValue || rightExp instanceof LongValue)
			selectionList.add(arg);
		else {
			String leftTable = leftExp.toString().split("\\.")[0];
			String rightTable = rightExp.toString().split("\\.")[0];
			if (leftTable.equals(rightTable))
				selectionList.add(arg);
			else
				joinList.add(arg);
		}
	}

	// ********** UNSUPPORTED OPERATIONS BELOW: DO NOT SUPPORT! **********

	@Override
	public void visit(Column arg) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(NullValue arg0) {
		throw new UnsupportedOperationException("An unsupported operation was called.");
	}

	@Override
	public void visit(LongValue arg) {
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
