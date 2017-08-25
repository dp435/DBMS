package DBMS;

import java.util.ArrayList;
import java.util.List;

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
 * Visitor for the union find algorithm.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class BuildUnionFindVisitor implements ExpressionVisitor {

	List<Expression> unusableExpressions;
	List<Expression> equalityJoinExpressions;
	UnionFind builtUnionFind;

	public BuildUnionFindVisitor() {
		unusableExpressions = new ArrayList<Expression>();
		equalityJoinExpressions = new ArrayList<Expression>();
		builtUnionFind = new UnionFind();
	}

	/**
	 * Method to get the result of evaluation of the expression tree when
	 * visitor is done.
	 * 
	 * @return the resulting union find database.
	 */
	public UnionFind getUnionedResult() {
		return builtUnionFind;
	}

	/**
	 * Method to get residual expressions.
	 * 
	 * @return the residual expressions.
	 */
	public Expression getUnusableExpression() {
		Expression unusableConditions = null;

		for (Expression exp : unusableExpressions) {
			if (unusableConditions == null)
				unusableConditions = exp;
			else
				unusableConditions = new AndExpression(unusableConditions, exp);
		}
		return unusableConditions;
	}

	/** Method to get all equality expressions. */
	public Expression getAllEqualityExpression() {
		Expression accumulatedExpression = null;

		for (Expression exp : equalityJoinExpressions) {
			if (accumulatedExpression == null)
				accumulatedExpression = exp;
			else
				accumulatedExpression = new AndExpression(accumulatedExpression, exp);
		}
		return accumulatedExpression;
	}



	/**
	 * Visit method for AndExpression node based on postorder tree traversal.
	 * 
	 * This method recursively evaluates its left and right expression
	 * subtree respectively.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(this);
		arg.getRightExpression().accept(this);
	}

	/**
	 * Visit method for GreaterThan node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThan arg) {
		if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof Column) {
			unusableExpressions.add(arg);
			return;
		} else if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof LongValue) {
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setLowerIfValid(Integer.parseInt(arg.getRightExpression().toString()) + 1);
		} else if (arg.getLeftExpression() instanceof LongValue && arg.getRightExpression() instanceof Column) {
			String att = arg.getRightExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setUpperIfValid(Integer.parseInt(arg.getLeftExpression().toString()) - 1);
		}
	}

	/**
	 * Visit method for GreaterThanEqual node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThanEquals arg) {
		if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof Column) {
			unusableExpressions.add(arg);
			return;
		} else if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof LongValue) {
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setLowerIfValid(Integer.parseInt(arg.getRightExpression().toString()));
		} else if (arg.getLeftExpression() instanceof LongValue && arg.getRightExpression() instanceof Column) {
			String att = arg.getRightExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setUpperIfValid(Integer.parseInt(arg.getLeftExpression().toString()));
		}
	}

	/**
	 * Visit method for MinorThan node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThan arg) {
		if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof Column) {
			unusableExpressions.add(arg);
			return;
		} else if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof LongValue) {
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setUpperIfValid(Integer.parseInt(arg.getRightExpression().toString()) - 1);
		} else if (arg.getLeftExpression() instanceof LongValue && arg.getRightExpression() instanceof Column) {
			String att = arg.getRightExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setLowerIfValid(Integer.parseInt(arg.getLeftExpression().toString()) + 1);
		}
	}

	/**
	 * Visit method for MinorThanEqual node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThanEquals arg) {
		if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof Column) {
			unusableExpressions.add(arg);
			return;
		} else if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof LongValue) {
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setUpperIfValid(Integer.parseInt(arg.getRightExpression().toString()));
		} else if (arg.getLeftExpression() instanceof LongValue && arg.getRightExpression() instanceof Column) {
			String att = arg.getRightExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.setLowerIfValid(Integer.parseInt(arg.getLeftExpression().toString()));
		}
	}

	/**
	 * Visit method for EqualsTo node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(EqualsTo arg) {
		if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof Column) {
			equalityJoinExpressions.add(arg);
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			UFElement temp2 = builtUnionFind.find(arg.getRightExpression().toString());
			builtUnionFind.union(temp, temp2);
		} else if (arg.getLeftExpression() instanceof Column && arg.getRightExpression() instanceof LongValue) {
			String att = arg.getLeftExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.upperBound = Integer.parseInt(arg.getRightExpression().toString());
			temp.lowerBound = Integer.parseInt(arg.getRightExpression().toString());
			temp.equalityConstraint = Integer.parseInt(arg.getRightExpression().toString());
		} else if (arg.getLeftExpression() instanceof LongValue && arg.getRightExpression() instanceof Column) {
			String att = arg.getRightExpression().toString();
			UFElement temp = builtUnionFind.find(att);
			temp.upperBound = Integer.parseInt(arg.getLeftExpression().toString());
			temp.lowerBound = Integer.parseInt(arg.getLeftExpression().toString());
			temp.equalityConstraint = Integer.parseInt(arg.getLeftExpression().toString());
		}
	}

	/**
	 * Visit method for NotEqualsTo node based on postorder tree traversal.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(NotEqualsTo arg) {
		unusableExpressions.add(arg);
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

	private boolean isNumber(String value) {
		try {
			Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return false;
		}
		return true;
	}

}
