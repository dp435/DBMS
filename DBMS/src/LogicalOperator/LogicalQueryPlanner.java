package LogicalOperator;

import DBMS.*;
import PhysicalOperator.EvaluateIndexExpressionVisitor;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Stack;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;

/**
 * This class is responsible for constructing the operator tree.
 *
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 */

public class LogicalQueryPlanner {

	private List selectClause;
	private String fromClause;
	private ArrayList<String> relationList;
	private Expression whereClause;
	private List orderByList;
	private List<String> outputOrder;

	private boolean distinct;
	private boolean joinRequired;
	private boolean usingAliases;

	private List<Expression> selectionList;
	private List<Expression> joinList;
	private JavaUtils utils;

	private String joinMethod;
	private int joinBuffer;
	private String sortMethod;
	private int sortBuffer;
	private DatabaseCatalog catalog;

	private boolean useIndexes;
	private LogicalOperator rootOperator;

	/**
	 * Constructor for QueryPlanner.
	 * 
	 * @param interpreter
	 *            An instance of an interpreter mainly used to parse the current
	 *            query.
	 */
	public LogicalQueryPlanner(Interpreter interpreter) {
		selectClause = interpreter.selectClause;
		fromClause = interpreter.fromClause.toString();
		relationList = new ArrayList<String>();
		relationList.add(fromClause);
		if (interpreter.fromClauseRest != null) {
			joinRequired = true;
			for (Join item : interpreter.fromClauseRest)
				relationList.add(item.toString());
		} else {
			joinRequired = false;
		}
		whereClause = interpreter.whereClause;
		distinct = interpreter.distinct;
		orderByList = interpreter.orderByList;
		outputOrder = interpreter.outputOrder;
		usingAliases = interpreter.usingAliases;

		joinBuffer = interpreter.joinBuffer;
		sortMethod = interpreter.sortMethod;
		sortBuffer = interpreter.sortBuffer;
		useIndexes = interpreter.useIndexes;

		catalog = DatabaseCatalog.getInstance();
		utils = new JavaUtils();

		rootOperator = null;
	}

	/**
	 * Method to construct the logical query plan.
	 */
	public LogicalOperator constructLogicalQueryPlan() {

		if (joinRequired) {
			rootOperator = constructJoinTree();
		}

		else {
			if (whereClause == null) {
				rootOperator = constructScanNode(fromClause);
			} else {
				rootOperator = constructSelectionNode(fromClause, whereClause);
			}
		}

		if (!selectClause.get(0).toString().equals("*")) {
			rootOperator = nestWithProjection(rootOperator);
		}

		if (orderByList != null) {
			rootOperator = nestWithSort(rootOperator, orderByList, outputOrder, sortMethod, sortBuffer, orderByList);
		}

		if (distinct) {
			if (orderByList != null)
				rootOperator = new LogicalDuplicateEliminationOperator(rootOperator);
			else {
				LogicalOperator sortOperator = nestWithSort(rootOperator, outputOrder, outputOrder, sortMethod,
						sortBuffer, orderByList);
				rootOperator = new LogicalDuplicateEliminationOperator(sortOperator);
			}
		}

		return rootOperator;
	}

	/**
	 * Method to construct the logical scan node.
	 * 
	 * @param fromExp
	 *            the FROM clause.
	 */
	public LogicalOperator constructScanNode(String fromExp) {
		String[] parsedRelationExp = fromExp.split(" ");
		String tablename = parsedRelationExp[0];
		String aliasname = null;
		if (usingAliases)
			aliasname = parsedRelationExp[2];

		LogicalOperator scanOperator = new LogicalScanOperator(tablename, aliasname);
		return scanOperator;
	}

	/**
	 * Method to construct the logical selection node.
	 * 
	 * @param fromExp
	 *            the FROM clause.
	 * @param selectionExp
	 *            the selection condition.
	 */
	public LogicalOperator constructSelectionNode(String fromExp, Expression selectionExp) {
		String[] parsedRelationExp = fromExp.split(" ");
		String tablename = parsedRelationExp[0];
		String aliasname = null;
		if (usingAliases)
			aliasname = parsedRelationExp[2];
		LogicalScanOperator scanOperator = new LogicalScanOperator(tablename, aliasname);
		LogicalSelectionOperator selectionOperator = new LogicalSelectionOperator(tablename, selectionExp, aliasname,
				scanOperator);
		return selectionOperator;
	}

	/**
	 * Method to nest a logical operator with a ProjectionOperator.
	 * 
	 * @param currentRootOperator
	 *            the operator to be nested with a ProjectionOperator.
	 */
	private LogicalOperator nestWithProjection(LogicalOperator currentRootOperator) {
		return new LogicalProjectionOperator(selectClause, currentRootOperator);
	}

	/**
	 * Method to nest a logical operator with a SortOperator.
	 * 
	 * @param currentRootOperator
	 *            the operator to be nested with a SortOperator.
	 */
	private LogicalOperator nestWithSort(LogicalOperator child, List orderByList, List<String> outputOrder,
			String sortMethod, int bufferCount, List<String> printList) {
		return new LogicalSortOperator(child, orderByList, outputOrder, sortMethod, sortBuffer, printList);
	}

	/**
	 * Helper method to construct a join operator tree.
	 */
	private LogicalOperator constructJoinTree() {
		LogicalJoinOperator joinOperator = new LogicalJoinOperator();

		// Case #1: not a null join.
		if (whereClause != null) {
			BuildUnionFindVisitor PrintUV = new BuildUnionFindVisitor();
			whereClause.accept(PrintUV);
			joinOperator.setUnionFindDatabase(PrintUV.getUnionedResult());

			BuildUnionFindVisitor UV = new BuildUnionFindVisitor();
			whereClause.accept(UV);
			UnionFind unionData = UV.getUnionedResult();

			EvaluateJoinVisitor JV = new EvaluateJoinVisitor();
			if (UV.getUnusableExpression() != null)
				UV.getUnusableExpression().accept(JV);

			List<Expression> residualJoins = JV.getJoinList();
			List<Expression> residualSelections = JV.getSelectionList();
			joinOperator.residualJoins = new ArrayList<Expression>(residualJoins);

			// Get all equality conditions where both left and right child are
			// instances of Column.
			Expression equalityExpressions = UV.getAllEqualityExpression();
			EvaluateJoinVisitor equalityJV = new EvaluateJoinVisitor();
			if (equalityExpressions != null)
				equalityExpressions.accept(equalityJV);

			List<Expression> rootJoinExpressions = equalityJV.getJoinList();
			List<Expression> equalitySelectionExpressions = equalityJV.getSelectionList();

			// Find any unpushed selections.
			for (Expression equalityExp : equalitySelectionExpressions) {
				String[] parsedExpression = equalityExp.toString().split(" = ");
				if (unionData.find(parsedExpression[0]).equalityConstraint == null
						&& unionData.find(parsedExpression[1]).equalityConstraint == null) {
					residualSelections.add(equalityExp);
				}
			}

			JavaUtils utils = new JavaUtils();

			for (int idx = 0; idx < relationList.size(); idx++) {
				String relation = relationList.get(idx);

				String[] parsedRelation = relation.split(" ");
				String tablename = parsedRelation[0];
				String aliasname = (!usingAliases) ? null : parsedRelation[2];

				List<String> attributeList = new ArrayList<String>(catalog.getAttributeInfo(tablename));
				if (!usingAliases)
					attributeList.replaceAll(att -> tablename + "." + att);
				else {
					attributeList.replaceAll(att -> aliasname + "." + att);
				}

				// For each attribute in a given relation, check if (geq/leq/eq)
				// conditions apply.
				Expression selectionConditionAccumulator = null;
				for (String attr : attributeList) {
					UFElement attrUnionData = unionData.find(attr);

					String[] parsedAttribute = attr.split("\\.");
					Table tab = new Table(null, parsedAttribute[0]);
					Column col = new Column(tab, parsedAttribute[1]);

					if (attrUnionData.equalityConstraint == null) {
						if (attrUnionData.lowerBound != null) {
							GreaterThanEquals geq = new GreaterThanEquals(col, new LongValue(attrUnionData.lowerBound));
							if (selectionConditionAccumulator == null)
								selectionConditionAccumulator = geq;
							else
								selectionConditionAccumulator = new AndExpression(selectionConditionAccumulator, geq);
						}
						if (attrUnionData.upperBound != null) {
							MinorThanEquals leq = new MinorThanEquals(col, new LongValue(attrUnionData.upperBound));
							if (selectionConditionAccumulator == null)
								selectionConditionAccumulator = leq;
							else
								selectionConditionAccumulator = new AndExpression(selectionConditionAccumulator, leq);
						}

					} else {
						EqualsTo eq = new EqualsTo(col, new LongValue(attrUnionData.equalityConstraint));
						if (selectionConditionAccumulator == null)
							selectionConditionAccumulator = eq;
						else
							selectionConditionAccumulator = new AndExpression(selectionConditionAccumulator, eq);
					}
				}

				// Check if current relation has some residual selections left
				// to apply.
				for (Expression residualSelection : residualSelections) {
					String nameUsedInExpression = !usingAliases ? tablename : aliasname;

					if (utils.contains(residualSelection.toString(), nameUsedInExpression)) {
						if (selectionConditionAccumulator == null)
							selectionConditionAccumulator = residualSelection;
						else
							selectionConditionAccumulator = new AndExpression(selectionConditionAccumulator,
									residualSelection);
					}
				}

				// No selection conditions for current relation; create scan
				// node.
				if (selectionConditionAccumulator == null) {
					joinOperator
							.addChildOperator(new LogicalJoinChild(tablename, aliasname, constructScanNode(relation)));
				}
				// Otherwise, create selection node.
				else {
					joinOperator.addChildOperator(new LogicalJoinChild(tablename, aliasname,
							constructSelectionNode(relation, selectionConditionAccumulator)));
				}
			}

			// Compile all equality join conditions to be placed at root.
			rootJoinExpressions.addAll(residualJoins);
			joinOperator.setJoinCondition(rootJoinExpressions);

		}

		// Case #2: WHERE expression is null.
		// Join everything using null conditions.
		else {
			for (int idx = 0; idx < relationList.size(); idx++) {
				String relation = relationList.get(idx);
				String[] parsedRelation = relation.split(" ");

				String tablename = parsedRelation[0];
				String aliasname = (!usingAliases) ? null : parsedRelation[2];

				joinOperator.addChildOperator(new LogicalJoinChild(tablename, aliasname, constructScanNode(relation)));
			}
		}

		return joinOperator;
	}

}
