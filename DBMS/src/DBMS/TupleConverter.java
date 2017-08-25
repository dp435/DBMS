package DBMS;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class is a utility class used to convert between the binary and
 * human-readable format for debugging purposes.
 * 
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class TupleConverter {

	/**
	 * This function takes in a human-readable file and outputs a binary file.
	 * 
	 * @param humanInput
	 *            File location of the human-readable input
	 * @param binaryOutput
	 *            File location for the binary output
	 */
	public void HumanToBinary(String humanInput, String binaryOutput) {
		TupleReaderHuman trh = new TupleReaderHuman(humanInput);
		TupleWriterBinary twb = new TupleWriterBinary(binaryOutput);
		FieldOrderedTuple fot;
		while ((fot = trh.readFOTuple()) != null) {
			twb.writeTuple(fot);
		}
		twb.flush();
	}

	/**
	 * This function takes in a binary file and outputs a human-readable file.
	 * 
	 * @param binaryInput
	 *            File location of the binary input
	 * @param humanOutput
	 *            File location for the human-readable output
	 */
	public void BinaryToHuman(String binaryInput, String humanOutput) {
		TupleReaderBinary trb = new TupleReaderBinary(binaryInput);
		TupleWriterHuman twh = null;
		try {
			twh = new TupleWriterHuman(humanOutput);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		FieldOrderedTuple fot;
		while ((fot = trb.readFOTuple()) != null) {
			twh.writeTuple(fot);
		}
		twh.close();
	}

	public void compress(String binaryFile) {
		BinaryToHuman(binaryFile, binaryFile + this.hashCode());
		try {
			File file = new File(binaryFile);
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
		HumanToBinary(binaryFile + this.hashCode(), binaryFile);
		try {
			File file = new File(binaryFile + this.hashCode());
			file.delete();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// BinaryToHuman(0)/HumanToBinary(1) inputfile outputfile
	public static void main(String[] args) {
		TupleConverter tc = new TupleConverter();
		tc.BinaryToHuman("./output/query1", "./output/query1_readable");
		tc.BinaryToHuman("./output/query2", "./output/query2_readable");
		tc.BinaryToHuman("./output/query3", "./output/query3_readable");
		tc.BinaryToHuman("./output/query4", "./output/query4_readable");
		tc.BinaryToHuman("./output/query5", "./output/query5_readable");
		tc.BinaryToHuman("./output/query6", "./output/query6_readable");
		tc.BinaryToHuman("./output/query7", "./output/query7_readable");
		tc.BinaryToHuman("./output/query8", "./output/query8_readable");
		tc.BinaryToHuman("./output/query9", "./output/query9_readable");
		tc.BinaryToHuman("./output/query10", "./output/query10_readable");
		tc.BinaryToHuman("./output/query11", "./output/query11_readable");
		tc.BinaryToHuman("./output/query12", "./output/query12_readable");
		tc.BinaryToHuman("./output/query13", "./output/query13_readable");
		tc.BinaryToHuman("./output/query14", "./output/query14_readable");
		tc.BinaryToHuman("./output/query15", "./output/query15_readable");
		
		tc.BinaryToHuman("./soloutput/query12", "./soloutput/query12_readable");
		tc.BinaryToHuman("./soloutput/query13", "./soloutput/query13_readable");
		tc.BinaryToHuman("./soloutput/query14", "./soloutput/query14_readable");
		tc.BinaryToHuman("./soloutput/query21", "./soloutput/query21_readable");

		tc.BinaryToHuman("./soloutputexpected/query12", "./soloutputexpected/query12_readable");
		tc.BinaryToHuman("./soloutputexpected/query13", "./soloutputexpected/query13_readable");
		tc.BinaryToHuman("./soloutputexpected/query14", "./soloutputexpected/query14_readable");
		tc.BinaryToHuman("./soloutputexpected/query21", "./soloutputexpected/query21_readable");	
/*
		if (args.length < 3) {
			System.out.println("Wrong args");
			return;
		}
		TupleConverter tc = new TupleConverter();
		if (Integer.parseInt(args[0]) == 0) {
			tc.BinaryToHuman(args[1], args[2]);
		} else if (Integer.parseInt(args[0]) == 1) {
			tc.HumanToBinary(args[1], args[2]);
		} else {
			System.out.println("Wrong args");
		}*/
	}

}
