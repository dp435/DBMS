package DBMS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import LogicalOperator.LogicalOperator;
import LogicalOperator.LogicalPlanPrinter;
import LogicalOperator.LogicalQueryPlanner;
import PhysicalOperator.Operator;
import PhysicalOperator.PhysicalPlanPrinter;
import PhysicalOperator.PhysicalQueryPlanner;

public class MainProcessor {
	/**
	 * The top-level class.
	 *
	 * @author Daniel Park (dp435) & Michael Neborak (mln45)
	 */

	/**
	 * Main method for MainProcessor.
	 * 
	 * This method is responsible for reading in the system arguments, setting
	 * up the interpreter and root operator, and writing the results of
	 * evaluating the query.
	 * 
	 * If an exception is encountered while evaluating a query, a blank file is
	 * written and the program proceeds to evaluate the next query, if such
	 * exists.
	 * 
	 * @param args
	 *            args[0] must be the input directory and args[1] must be the
	 *            output directory.
	 */
	public static void main(String[] args) {
		String configPath = args[0];

		BufferedReader configReader = null;
		try {
			configReader = new BufferedReader(new FileReader(new File(configPath)));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		String inputDirectory = null;
		String outputDirectory = null;
		String tempDirectory = null;
		boolean buildIndexes = true;
		boolean evaluateQueries = true;
		try {
			inputDirectory = configReader.readLine();
			outputDirectory = configReader.readLine();
			tempDirectory = configReader.readLine();
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		int filenumber = 1;
		TupleWriterHuman tupleWriterHuman = null;
		TupleWriterBinary tupleWriterBinary = null;

		Interpreter interpreter = new Interpreter(inputDirectory, outputDirectory, tempDirectory, buildIndexes,
				evaluateQueries);

		if (evaluateQueries) {

			while (interpreter.EOF == false) {
				long starttime = System.currentTimeMillis();
				try {

					interpreter.readline();
					if (interpreter.EOF == true) {
						return;
					}

					tupleWriterBinary = new TupleWriterBinary(outputDirectory + "/query" + filenumber);
					tupleWriterHuman = new TupleWriterHuman(outputDirectory + "/query" + filenumber);

					LogicalQueryPlanner logicalPlan = new LogicalQueryPlanner(interpreter);

					LogicalOperator rootLogicalOperator = logicalPlan.constructLogicalQueryPlan();
					LogicalPlanPrinter logicalPrinter = new LogicalPlanPrinter();
					rootLogicalOperator.accept(logicalPrinter);

					try (Writer writer = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(outputDirectory + "/query" + filenumber + "_logicalplan"), "utf-8"))) {
						writer.write(logicalPrinter.printLogicalTree());
						writer.close();
					}

					PhysicalQueryPlanner physicalPlan = new PhysicalQueryPlanner(logicalPlan);
					Operator planner = physicalPlan.getPhysicalPlan();
					
					PhysicalPlanPrinter physicalPrinter = new PhysicalPlanPrinter();
					planner.accept(physicalPrinter);
					
					try (Writer writer = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(outputDirectory + "/query" + filenumber + "_physicalplan"), "utf-8"))) {
						writer.write(physicalPrinter.printPhysicalTree());
						writer.close();
					}

					FieldOrderedTuple fot;
					List<Integer> values = new ArrayList<Integer>();

					Tuple resultTuple;
					while ((resultTuple = planner.getNextTuple()) != null) {
						values.clear();
						for (String fieldname : interpreter.outputOrder) {
							values.add(Integer.parseInt(resultTuple.getField(fieldname)));
						}
						fot = new FieldOrderedTuple(values);
						tupleWriterBinary.writeTuple(fot);

					}
					tupleWriterBinary.flush();
					tupleWriterBinary.close();
					filenumber++;

					File tempFolder = new File(DatabaseCatalog.getInstance().getTempDirectory());
					File[] files = tempFolder.listFiles();
					if (files != null)
						for (File f : files)
							if (!f.isDirectory())
								f.delete();

				} catch (Exception e) {
					// Fail-safe: close writer if open and increment filenumber
					// counter.
					e.printStackTrace();
					System.out.println("Exception encountered: skipping to next query.");
					if (tupleWriterHuman != null)
						tupleWriterHuman.close();
					filenumber++;
				}

				long endtime = System.currentTimeMillis();
				// System.out.println(endtime - starttime);
			}
		}

	}
}
