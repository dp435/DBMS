package DBMS;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Class to write a tuple to a binary file.
 * @author Daniel Park (dp435) & Michael Neborak (mln45)
 *
 */
public class TupleWriterBinary {

	String fileLocation;
	ByteBuffer pageInMemory;
	FileOutputStream fin;
	FileChannel fc;
	int pageIndex = 0;
	public static final int TUPLE_SIZE_INDEX = 0;
	public static final int TUPLE_COUNT_INDEX = 4;
	public static final int ELEMENT_SIZE = 4;
	int tuplesOnPage = 0;
	boolean forcedFlush = false;

	/**
	 * Constructor for the TupleWriterBinary
	 * @param fileLocation The associated file to write tuples to
	 */
	public TupleWriterBinary(String fileLocation) {
		this.fileLocation = fileLocation;
		try {
			fin = new FileOutputStream(fileLocation);
			fc = fin.getChannel();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write all tuples in in-memory buffer to disk.
	 */
	public void flush() {
		try {
			if (pageInMemory != null){
			fc.write(pageInMemory);
			forcedFlush = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Close the associated file on disk.
	 */
	public void close() {
		try {
			fc.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Write the specified tuple to the binary file.
	 * @param tuple The tuple to be written.
	 */
	public void writeTuple(FieldOrderedTuple tuple) {
		if (tuple == null) {
			return;
		}
		try {
			if (pageIndex == 0) {
				pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
				pageInMemory.putInt(TUPLE_SIZE_INDEX, tuple.size());
				pageInMemory.putInt(TUPLE_COUNT_INDEX, 1);
				pageIndex = 8;
				tuplesOnPage = 1;
				for (int i = 0; i < tuple.size(); i++) {
					pageInMemory.putInt(pageIndex, tuple.getField(i));
					pageIndex += ELEMENT_SIZE;
				}
			} else if (pageIndex + tuple.size() * ELEMENT_SIZE > TupleReaderBinary.BUFFER_SIZE || forcedFlush) {
				// System.out.println("FLUSHING");
				forcedFlush = false;
				fc.write(pageInMemory);
				pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
				pageInMemory.putInt(TUPLE_SIZE_INDEX, tuple.size());
				pageInMemory.putInt(TUPLE_COUNT_INDEX, 1);
				pageIndex = 8;
				tuplesOnPage = 1;
				for (int i = 0; i < tuple.size(); i++) {
					pageInMemory.putInt(pageIndex, tuple.getField(i));
					pageIndex += ELEMENT_SIZE;
				}
			} else {
				for (int i = 0; i < tuple.size(); i++) {
					pageInMemory.putInt(pageIndex, tuple.getField(i));
					pageIndex += ELEMENT_SIZE;
				}
				tuplesOnPage++;
				pageInMemory.putInt(TUPLE_COUNT_INDEX, tuplesOnPage);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void writeBlankPage() {
		pageInMemory = ByteBuffer.allocate(TupleReaderBinary.BUFFER_SIZE);
		pageInMemory.putInt(TUPLE_SIZE_INDEX, 0);
		pageInMemory.putInt(TUPLE_COUNT_INDEX, 0);
	}

}
