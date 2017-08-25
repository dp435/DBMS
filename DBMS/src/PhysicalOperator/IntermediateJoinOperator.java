package PhysicalOperator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;

import DBMS.Tuple;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/**
 * Intermediate class to calculate intermediate join sizes for dynamic-programming-based-join-tree-creation.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class IntermediateJoinOperator implements DJoinOperator{
	
	DJoinOperator leftChild;
	DJoinOperator rightChild;
	
	//This is the "cost of the left child (zero in this case)"
	int cost = 0;
	
	//This is "the size of the relation produced as the result of the left child, i.e. R /\ S."
	int size = 0;
	
	Expression joinExpression;
	Expression equalityJoinExpression;
	ArrayList<Expression> equalityJoinList = new ArrayList<>();
	HashSet<String> joinedTables = new HashSet<>();
	
	Hashtable<String, Integer> vHTable = new Hashtable<>();
	
	Hashtable<String, Integer> joinHTable = new Hashtable<>();
	
	/**
	 * Constructor to create an IntermediateJoinOperator.
	 * @param leftChild Left tree or Base relation
	 * @param rightChild Right base relation
	 */
	public IntermediateJoinOperator(DJoinOperator leftChild, DJoinOperator rightChild){
		this.leftChild = leftChild;
		this.rightChild = rightChild;
	}
	
	/**
	 * Adds a join expression
	 * @param joinExpression Expression to add
	 */
	public void addJoinExpression(Expression joinExpression){
		if (this.joinExpression == null){
			this.joinExpression = joinExpression;
		}else{
			this.joinExpression = new AndExpression(this.joinExpression, joinExpression);
		}
	}
	
	/**
	 * Adds a join expression of the EqualsTo type
	 * @param joinExpression EqualsTo Expression
	 */
	public void addEqualityJoinExpression(Expression joinExpression){
		if (this.equalityJoinExpression == null){
			this.equalityJoinExpression = joinExpression;
		}else{
			this.equalityJoinExpression = new AndExpression(this.equalityJoinExpression, joinExpression);
		}
	}
	
	/**
	 * Adds an EqualsTo Expression to a list 
	 * Helper function to allow list to be iterable
	 * @param exp EqualsTo Expression
	 */
	public void addEqualityJoinList(Expression exp){
		equalityJoinList.add(exp);
	}
	
	/**
	 * Adds an attribute to the Base V-Value table
	 * @param attribute Attribute to add
	 * @param vValue Value of attribute
	 */
	public void addToVHT(String attribute, int vValue){
		vHTable.put(attribute, vValue);
	}
	
	/**
	 * Adds an attribute to the Join V-Value table
	 * @param attribute Attribute to add
	 * @param vValue Value of attribute
	 */
	public void addToJHT(String attribute, int vValue){
		joinHTable.put(attribute, vValue);
	}
	
	/**
	 * Gets the value of an attribute from the
	 * Base V-Value table.
	 * @param attribute Attribute to retrieve
	 * @return Attribute's value
	 */
	public int getFromVHT(String attribute){
		return vHTable.get(attribute);
	}
	
	/**
	 * Gets the value of an attribute from the
	 * Join V-Value table.
	 * @param attribute Attribute to retrieve
	 * @return Attribute's value
	 */
	public int getFromJHT(String attribute){
		return joinHTable.get(attribute);
	}

}
