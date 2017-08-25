package PhysicalOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import DBMS.DatabaseCatalog;
import DBMS.IndexInfo;
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

public class EvaluateIndexExpressionVisitor implements ExpressionVisitor {

	private String tablename;
	private String aliasname;
	private boolean nullifyKeys;
	private Expression indexedExpression;
	private Expression nonIndexedExpression;
	private EvaluateIndexExpressionVisitor EV;
	private String indexedAttribute;
	private Stack<Long> longStack;
	private List<Long> lowCandidateKeys;
	private List<Long> highCandidateKeys;

	/**
	 * Constructor for EvaluateIndexExpressionVisitor.
	 * 
	 * @param tablename
	 *            the name of the current relation
	 * @param aliasname
	 *            the alias name to the current relation
	 */
	public EvaluateIndexExpressionVisitor(String tablename, String aliasname, String indexedAttribute) {
		this.tablename = tablename;
		this.aliasname = aliasname;
		indexedExpression = null;
		nonIndexedExpression = null;
		nullifyKeys = false;
		EV = this;
		longStack = new Stack<Long>();
		lowCandidateKeys = new ArrayList<Long>();
		highCandidateKeys = new ArrayList<Long>();
		if (aliasname != null)
			this.indexedAttribute = aliasname + "." + indexedAttribute;
		else
			this.indexedAttribute = tablename + "." + indexedAttribute;
	}

	/**
	 * Method to get the subset of expressions involving indexed attributes when
	 * visitor is done.
	 * 
	 * @return an expression involving indexed attributes.
	 */
	public Expression getIndexedExpression() {
		return indexedExpression;
	}

	/**
	 * Method to get the subset of expressions involving unindexed attributes
	 * when visitor is done.
	 * 
	 * @return an expression involving unindexed attributes.
	 */
	public Expression getNonIndexedExpression() {
		return nonIndexedExpression;
	}

	public Integer getLowkey() {
		try {
			return Collections.min(lowCandidateKeys).intValue();
		} catch (Exception NoSuchElementException) {
			return null;
		}
	}

	public Integer getHighkey() {
		try {
			return Collections.max(highCandidateKeys).intValue();
		} catch (Exception NoSuchElementException) {
			return null;
		}
	}

	/**
	 * Visit method for AndExpression node based on postorder tree traversal.
	 * 
	 * This method first recursively evaluates its left and right expression
	 * subtree respectively.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(AndExpression arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);
	}

	/**
	 * Visit method for GreaterThan node based on postorder tree traversal.
	 * 
	 * This method checks to see if either of its child equals an indexed
	 * attribute. If so, the current expression is added to indexedExpression;
	 * else, it is added to nonIndexedExpression.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThan arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);

		String leftChild = arg.getLeftExpression().toString();
		String rightChild = arg.getRightExpression().toString();

		if (arg.getLeftExpression() instanceof LongValue || arg.getRightExpression() instanceof LongValue) {
			if (indexedAttribute.equals(leftChild)) {
				lowCandidateKeys.add(longStack.pop() + 1);
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else if (indexedAttribute.equals(rightChild)) {
				highCandidateKeys.add(longStack.pop() - 1);
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else {
				if (nonIndexedExpression == null)
					nonIndexedExpression = arg;
				else
					nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
			}
		} else {
			if (nonIndexedExpression == null)
				nonIndexedExpression = arg;
			else
				nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
		}
	}

	/**
	 * Visit method for GreaterThanEqual node based on postorder tree traversal.
	 * 
	 * This method checks to see if either of its child equals an indexed
	 * attribute. If so, the current expression is added to indexedExpression;
	 * else, it is added to nonIndexedExpression.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(GreaterThanEquals arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);

		String leftChild = arg.getLeftExpression().toString();
		String rightChild = arg.getRightExpression().toString();

		if (arg.getLeftExpression() instanceof LongValue || arg.getRightExpression() instanceof LongValue) {
			if (indexedAttribute.equals(leftChild)) {
				lowCandidateKeys.add(longStack.pop());
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else if (indexedAttribute.equals(rightChild)) {
				highCandidateKeys.add(longStack.pop());
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else {
				if (nonIndexedExpression == null)
					nonIndexedExpression = arg;
				else
					nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
			}
		} else {
			if (nonIndexedExpression == null)
				nonIndexedExpression = arg;
			else
				nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
		}
	}

	/**
	 * Visit method for MinorThan node based on postorder tree traversal.
	 * 
	 * This method checks to see if either of its child equals an indexed
	 * attribute. If so, the current expression is added to indexedExpression;
	 * else, it is added to nonIndexedExpression.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThan arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);

		String leftChild = arg.getLeftExpression().toString();
		String rightChild = arg.getRightExpression().toString();

		if (arg.getLeftExpression() instanceof LongValue || arg.getRightExpression() instanceof LongValue) {
			if (indexedAttribute.equals(leftChild)) {
				highCandidateKeys.add(longStack.pop() - 1);
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else if (indexedAttribute.equals(rightChild)) {
				lowCandidateKeys.add(longStack.pop() + 1);
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else {
				if (nonIndexedExpression == null)
					nonIndexedExpression = arg;
				else
					nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
			}
		} else {
			if (nonIndexedExpression == null)
				nonIndexedExpression = arg;
			else
				nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
		}
	}

	/**
	 * Visit method for MinorThanEqual node based on postorder tree traversal.
	 * 
	 * This method checks to see if either of its child equals an indexed
	 * attribute. If so, the current expression is added to indexedExpression;
	 * else, it is added to nonIndexedExpression.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(MinorThanEquals arg) {
		arg.getLeftExpression().accept(EV);
		arg.getRightExpression().accept(EV);

		String leftChild = arg.getLeftExpression().toString();
		String rightChild = arg.getRightExpression().toString();

		if (arg.getLeftExpression() instanceof LongValue || arg.getRightExpression() instanceof LongValue) {
			if (indexedAttribute.equals(leftChild)) {
				highCandidateKeys.add(longStack.pop());
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else if (indexedAttribute.equals(rightChild)) {
				lowCandidateKeys.add(longStack.pop());
				if (indexedExpression == null)
					indexedExpression = arg;
				else
					indexedExpression = new AndExpression(indexedExpression, arg);
			} else {
				if (nonIndexedExpression == null)
					nonIndexedExpression = arg;
				else
					nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
			}
		} else {
			if (nonIndexedExpression == null)
				nonIndexedExpression = arg;
			else
				nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
		}
	}

	/**
	 * Visit method for EqualsTo node based on postorder tree traversal.
	 * 
	 * The EqualsTo operator cannot make use of any indexed attributes.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(EqualsTo arg) {
		if (nonIndexedExpression == null)
			nonIndexedExpression = arg;
		else
			nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
	}

	/**
	 * Visit method for NotEqualsTo node based on postorder tree traversal.
	 * 
	 * The NotEqualsTo operator cannot make use of any indexed attributes.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(NotEqualsTo arg) {
		if (nonIndexedExpression == null)
			nonIndexedExpression = arg;
		else
			nonIndexedExpression = new AndExpression(nonIndexedExpression, arg);
	}

	/**
	 * Visit method for NotEqualsTo node based on postorder tree traversal.
	 * 
	 * The NotEqualsTo operator cannot make use of any indexed attributes.
	 * 
	 * @param arg
	 *            the node to be visited
	 */
	@Override
	public void visit(LongValue arg) {
		longStack.push(arg.toLong());
	}

	@Override
	public void visit(Column arg) {
		return;
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
