/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.mahout.classifier.sgd;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.stats.OnlineSummarizer;
import org.apache.mahout.vectorizer.encoders.ConstantValueEncoder;
import org.apache.mahout.vectorizer.encoders.FeatureVectorEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

/**
 * Shows how different encoding choices can make big speed differences.
 * <p/>
 * Run with command line options --generate 1000000 test.csv to generate a million data lines in
 * test.csv.
 * <p/>
 * Run with command line options --parser test.csv to time how long it takes to parse and encode
 * those million data points
 * <p/>
 * Run with command line options --fast test.csv to time how long it takes to parse and encode those
 * million data points using byte-level parsing and direct value encoding.
 * <p/>
 * This doesn't demonstrate text encoding which is subject to somewhat different tricks.  The basic
 * idea of caching hash locations and byte level parsing still very much applies to text, however.
 */
public final class SimpleCsvExamples {

  public static final char SEPARATOR_CHAR = '\t';
  private static final int FIELDS = 100;

  private static final Logger log = LoggerFactory.getLogger(SimpleCsvExamples.class);

  private SimpleCsvExamples() {}

  public static void main(String[] args) throws IOException {
    FeatureVectorEncoder[] encoder = new FeatureVectorEncoder[FIELDS];
    for (int i = 0; i < FIELDS; i++) {
      encoder[i] = new ConstantValueEncoder("v" + 1);
    }

    OnlineSummarizer[] s = new OnlineSummarizer[FIELDS];
    for (int i = 0; i < FIELDS; i++) {
      s[i] = new OnlineSummarizer();
    }
    long t0 = System.currentTimeMillis();
    Vector v = new DenseVector(1000);
    if ("--generate".equals(args[0])) {
      PrintWriter out =
          new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(args[2])), Charsets.UTF_8));
      try {
        int n = Integer.parseInt(args[1]);
        for (int i = 0; i < n; i++) {
          Line x = Line.generate();
          out.println(x);
        }
      } finally {
        Closeables.close(out, false);
      }
    } else if ("--parse".equals(args[0])) {
      BufferedReader in = Files.newReader(new File(args[1]), Charsets.UTF_8);
      try {
        String line = in.readLine();
        while (line != null) {
          v.assign(0);
          Line x = new Line(line);
          for (int i = 0; i < FIELDS; i++) {
            s[i].add(x.getDouble(i));
            encoder[i].addToVector(x.get(i), v);
          }
          line = in.readLine();
        }
      } finally {
        Closeables.close(in, true);
      }
      String separator = "";
      for (int i = 0; i < FIELDS; i++) {
        System.out.printf("%s%.3f", separator, s[i].getMean());
        separator = ",";
      }
    } else if ("--fast".equals(args[0])) {
      FastLineReader in = new FastLineReader(new FileInputStream(args[1]));
      try {
        FastLine line = in.read();
        while (line != null) {
          v.assign(0);
          for (int i = 0; i < FIELDS; i++) {
            double z = line.getDouble(i);
            s[i].add(z);
            encoder[i].addToVector((byte[]) null, z, v);
          }
          line = in.read();
        }
      } finally {
        Closeables.close(in, true);
      }
      String separator = "";
      for (int i = 0; i < FIELDS; i++) {
        System.out.printf("%s%.3f", separator, s[i].getMean());
        separator = ",";
      }
    }
    System.out.printf("\nElapsed time = %.3f%n", (System.currentTimeMillis() - t0) / 1000.0);
  }


  private static final class Line {
    private static final Splitter ON_TABS = Splitter.on(SEPARATOR_CHAR).trimResults();
    public static final Joiner WITH_COMMAS = Joiner.on(SEPARATOR_CHAR);

    public static final Random RAND = RandomUtils.getRandom();

    private final List<String> data;

    private Line(CharSequence line) {
      data = Lists.newArrayList(ON_TABS.split(line));
    }

    private Line() {
      data = Lists.newArrayList();
    }

    public double getDouble(int field) {
      return Double.parseDouble(data.get(field));
    }

    /**
     * Generate a random line with 20 fields each with integer values.
     *
     * @return A new line with data.
     */
    public static Line generate() {
      Line r = new Line();
      for (int i = 0; i < FIELDS; i++) {
        double mean = ((i + 1) * 257) % 50 + 1;
        r.data.add(Integer.toString(randomValue(mean)));
      }
      return r;
    }

    /**
     * Returns a random exponentially distributed integer with a particular mean value.  This is
     * just a way to create more small numbers than big numbers.
     *
     * @param mean
     * @return
     */
    private static int randomValue(double mean) {
      return (int) (-mean * Math.log1p(-RAND.nextDouble()));
    }

    @Override
    public String toString() {
      return WITH_COMMAS.join(data);
    }

    public String get(int field) {
      return data.get(field);
    }
  }

      }
    }
  }
  public static final class FastLine {
	    public static final char SEPARATOR_CHAR = ',';

	    private final ByteBuffer base;
	    private final IntArrayList start = new IntArrayList();
	    private final IntArrayList length = new IntArrayList();
	    private int numFields = 0;

	    private FastLine(ByteBuffer base) {
	      this.base = base;
	    }

	    public int getNumFields()
	    {
	    	return numFields;
	    }
	    
	    public static FastLine read(ByteBuffer buf) {
	      FastLine r = new FastLine(buf);
	      r.start.add(buf.position());
	      int offset = buf.position();
	      while (offset < buf.limit()) {
	        int ch = buf.get();
	        offset = buf.position();
	        switch (ch) {
	          case '\n':
	            r.length.add(offset - r.start.get(r.length.size()) - 1);
	            r.numFields++;
	            return r;
	          case SEPARATOR_CHAR:
	            r.length.add(offset - r.start.get(r.length.size()) - 1);
	            r.start.add(offset);
	            r.numFields++;
	            break;
	          default:
	        	if(offset == buf.limit()) {
	        		r.length.add(offset - r.start.get(r.length.size()));
	        		r.numFields++;
	                return r;
	        	}
	            // nothing to do for now
	        }
	      }
	      throw new IllegalArgumentException("Not enough bytes in buffer");
	    }

	    public double getDouble(int field, int startPosition)
	    {
	        int offset = start.get(field);
	        int size = length.get(field);
	        switch (size) {
	          case 1:
	            return base.get(offset) - '0';
	          case 2:
	        	  if(base.get(offset) == '-')
	        		  return -(base.get(offset + 1) - '0');
	        	  else
	        		  return (base.get(offset) - '0') * 10 + base.get(offset + 1) - '0';
	          default:
	          {
	            double r = 0.0, factor1 = 10.0, rFraction = 0.0;
	            int i = startPosition;
	            byte b = 0;
	            boolean negative = base.get(offset + i) == '-' ? true : false;
	            if(negative)
	            	i++;
	            for (; i < size; i++) {
	          	  b = base.get(offset + i);
	          	  if(!Character.isDigit(b)) {
	          		  break;
	          	  }
	                r = factor1 * r + b - '0';
	            }
	            if(b != '.') {
	          	  return negative ? -r : r;
	            }
	            
	            int numFractionDigits = 0;
	            for(i++;i < size;i++) {
	          	  b = base.get(offset + i);
	          	  if(!Character.isDigit(b)) {
	          		  break;
	          	  }
	                rFraction = factor1 * rFraction + b - '0';
	                numFractionDigits++;
	            }
	            rFraction *= Math.pow(10, -numFractionDigits);
	            r += rFraction;
	            return negative ? -r : r;
	          }
	        }    	
	    }
	    
	    public double getDouble(int field) {
	    	return getDouble(field, 0);
	    }
	    
	    public Object[] getPositionValue(int field) {
	        int offset = start.get(field);
	        int size = length.get(field);
	        switch (size) {
	          case 1:
	          {
	        	  Object[] ret = new Object[1];
	        	  ret[0] = (double) (base.get(offset) - '0');
	              return ret;
	          }
	          case 2:
	          {
	        	  Object[] ret = new Object[1];
	        	  if(base.get(offset) == '-')
	        		  ret[0] = -(base.get(offset + 1) - '0');
	        	  else
	        		  ret[0] = (double) ((base.get(offset) - '0') * 10 + base.get(offset + 1) - '0');
	              return ret;
	          }
	          default:
	          {
	            double r = 0.0, factor1 = 10.0, rFraction = 0.0;
	            int i = 0;
	            byte b = 0;
	            boolean negative = base.get(offset + i) == '-' ? true : false;
	            if(negative)
	            	i++;
	            for (; i < size; i++) {
	          	    b = base.get(offset + i);
	          	    if(!Character.isDigit(b)) {
	          		    break;
	          	    }
	                r = factor1 * r + b - '0';
	            }
	            
	            switch(b) {
	            case ':':
	            	 {
	            		Object[] ret = new Object[2];
	            		 // should not be negative since this is index into a sparse vector
	            		ret[0] = (int) r; // negative ? -r : r;
	            		ret[1] = getDouble(field, i+1);
	            		return ret;
	            	 }
	            case '.':
		            {
		                int numFractionDigits = 0;
		                for(i++;i < size;i++) {
		              	    b = base.get(offset + i);
		              	    if(!Character.isDigit(b)) {
		              		    break;
		              	    }
		                    rFraction = factor1 * rFraction + b - '0';
		                    numFractionDigits++;
		                }
		                rFraction *= Math.pow(10, -numFractionDigits);
		                r += rFraction;
		                Object[] ret = new Object[1];
		                ret[0] = negative ? -r : r;
		                return ret;
		            }
		        default:
			        {
		            	Object[] ret = new Object[1];
		            	ret[0] = negative ? -r : r;
		          	    return ret;
			        }
		            	
	            }
	          }
	        }
	      }

	  }

  public static final class FastLineReader implements Closeable {
	    private final InputStream in;
	    private ByteBuffer buf;

	    public FastLineReader(InputStream in, int capacity) throws IOException {
	        this.in = in;
	        buf = ByteBuffer.allocate(capacity);
	        buf.limit(0);
	        fillBuffer();    	
	    }
	    
	    public FastLineReader(InputStream in) throws IOException {
	    	this(in, 100000);
	    }

	    public FastLine read() throws IOException {
	      fillBuffer();
	      if (buf.remaining() > 0) {
	        return FastLine.read(buf);
	      } else {
	        return null;
	      }
	    }

	    public FastLine readLine() throws IOException {
	    	return read();
	    }
	    
	    private void fillBuffer() throws IOException {
	      if (buf.remaining() < 10000) {
	        buf.compact();
	        int n = in.read(buf.array(), buf.position(), buf.remaining());
	        if (n == -1) {
	          buf.flip();
	        } else {
	          buf.limit(buf.position() + n);
	          buf.position(0);
	        }
	      }
	    }

	    @Override
	    public void close() {
	      try {
	        Closeables.close(in, true);
	      } catch (IOException e) {
	      }
	    }
	    
	    @SuppressWarnings("unused")
		public static void testFastFileReader() throws IOException
	    {
	    	FastLineReader in = new FastLineReader(new FileInputStream("src/test/resources/intro.csv"));
	    	FastLine line = in.read();
	        while (line != null) {
	    		for (int i = 0; i < (line.getNumFields()-1); i++) {
	    			double z = line.getDouble(i);
	    			System.out.print(" " + z + ",");
	    		}
	    		double z = line.getDouble(line.getNumFields()-1);
	    		System.out.println(" " + z);
	    		line = in.read();
	        }
	        in.close();
	        
	        long startTime = System.currentTimeMillis();
	        File f = new File("src/test/resources/news-groups-vectors.txt");
	        in = new FastLineReader(new FileInputStream(f), (int) f.length());
	    	line = in.read();
	    	int lineNo = 1;
	        while (line != null ) {
	    		for (int i = 0; i < (line.getNumFields()-1); i++) {
	    			Object[] posVal  = line.getPositionValue(i);
	    			//System.out.print(" " + posVal[0] + ": " + posVal[1]);
	    		}
	    		Object[] posVal = line.getPositionValue(line.getNumFields()-1);
	    		//System.out.println(" " + posVal[0] + ": " + posVal[1]);
	    		try {
	    			line = in.read();
	    		}
	    		catch(Exception ex) {
	    			System.err.println("Error in processing line: " + lineNo);
	    		}
	    		lineNo++;
	        }
	        in.close();
	        long endTime = System.currentTimeMillis();
	        System.out.println("Time to process colon-comma separated file (FastLineReader): " + (endTime-startTime)/1000 + "s");
	        startTime = System.currentTimeMillis();
	        BufferedReader bufIn = new BufferedReader(new java.io.FileReader(f));
	        try {
	            String line1 = bufIn.readLine();
	            lineNo = 1;
	            while (line1 != null) {
	            	String[] parts = line1.split(",");
	            	int actual = -1;
	      	      for (int i = 0; i < (parts.length-1); i++) {
	    	  	      int index = i+1;
	    	  	      double z = 0.0;
	    	  	      if(parts[i].contains(":")) {
	    	  		      String[] pair = parts[i].split(":");
	    	  		      index = Integer.parseInt(pair[0]);
	    	  		      z = Double.parseDouble(pair[1]);
	    	  	      }
	    	  	      else {
	    	  		      z = Double.parseDouble(parts[i]);
	    	  	      }
	    	      }
	    	      if(parts.length >= 1) {
	    	  	      if(parts[parts.length-1].contains(":")) {
	    	  		      actual = (int) Double.parseDouble(parts[parts.length-1].split(":")[1]);
	    	  	      }
	    	  	      else {
	    	  		      actual = (int) Double.parseDouble(parts[parts.length-1]);
	    	  	      }
	    	  	      }

	                line1 = bufIn.readLine();
	                lineNo++;
	            }
	        } catch(IOException ex) {
	        	
	        }
	        bufIn.close();
	        endTime = System.currentTimeMillis();
	        System.out.println("Time to process colon-comma separated file (FileReader): " + (endTime-startTime)/1000 + "s");
	    }
	  }

}
