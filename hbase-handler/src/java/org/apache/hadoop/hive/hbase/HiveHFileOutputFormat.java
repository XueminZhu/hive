/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.FileWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


/**
 * HiveHFileOutputFormat implements HiveOutputFormat for HFile bulk
 * loading.  Until HBASE-1861 is implemented, it can only be used
 * for loading a table with a single column family.
 */
public class HiveHFileOutputFormat extends
    HFileOutputFormat implements
    HiveOutputFormat<ImmutableBytesWritable, KeyValue> {

  private static final String HFILE_FAMILY_PATH = "hfile.family.path";
   private static final String HIVE_DB_NAME = "hive.db.name";
   private static final String HIVE_TABLE_NAME = "hive.table.name";
 private static final String HIVE_MYSQL_URL = "hive.mysql.url";
 private static final String HIVE_MYSQL_USER = "hive.mysql.user";
 private static final String HIVE_MYSQL_PASS = "hive.mysql.pass";

  static final Log LOG = LogFactory.getLog(
    HiveHFileOutputFormat.class.getName());

  private
  org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable, KeyValue>
  getFileWriter(org.apache.hadoop.mapreduce.TaskAttemptContext tac)
  throws IOException {
    try {
      return super.getRecordWriter(tac);
    } catch (InterruptedException ex) {
      throw new IOException(ex);
    }
  }
  
  public static void getColumnTypes(String dbName,String tableName,Map<String,String> columnTypes,String mysqlurl,String username,String password){

	      String user = username;
	  	  //String password = password;
	  	  String url = mysqlurl;
	  	  String driver = "com.mysql.jdbc.Driver";
	  	  String sqlstr;
	  	  Connection con = null;
	  	  Statement stmt = null;
	  	  ResultSet rs = null;
	  	  try {
	  		  Class.forName(driver);
	  		  con = DriverManager.getConnection(url, user, password);
	  		  stmt = con.createStatement();
	  		  sqlstr = " select t1.tbl_name,t2.COLUMN_NAME,t2.TYPE_NAME from tbls t1,columns_v2 t2 ,sds t3,dbs t4 where t1.sd_id=t3.sd_id and t2.cd_id=t3.cd_id and t4.db_id=t1.db_id and t1.tbl_name='"+tableName+"' and t4.name='"+dbName+"';";
	  		  rs = stmt.executeQuery(sqlstr);
	  		  while(rs.next()){
	  			  String columnName = rs.getString( 2 );
	  			  String columnType = rs.getString( 3 );
				  LOG.info(" columnName is->>"+columnName+"    columnType is --->>" + columnType);
	  			  columnTypes.put( columnName, columnType );
	  		  }
	  		  
	  	  } catch( Exception e ) {
	  	     e.printStackTrace();     
	  	  }finally{
			try {
				rs.close();
				stmt.close();
				con.close();
			} catch( Exception e5 ) {
				e5.printStackTrace();
			}
		  }
	    }

  @Override
  public RecordWriter getHiveRecordWriter(
    final JobConf jc,
    final Path finalOutPath,
    Class<? extends Writable> valueClass,
    boolean isCompressed,
    Properties tableProperties,
    final Progressable progressable) throws IOException {
   


    // Read configuration for the target path
    String hfilePath = tableProperties.getProperty(HFILE_FAMILY_PATH);
    if (hfilePath == null) {
      throw new RuntimeException(
        "Please set " + HFILE_FAMILY_PATH + " to target location for HFiles");
    }


	String dbName = tableProperties.getProperty(HIVE_DB_NAME);
    if (dbName == null) {
	    throw new RuntimeException( "Please set " + HIVE_DB_NAME + " to get table columns type ");
	}
    LOG.info("dbName is ------------------>>"+dbName);
    
    String tableName = tableProperties.getProperty(HIVE_TABLE_NAME);
    if (tableName == null) {
	   throw new RuntimeException( "Please set " + HIVE_TABLE_NAME + " to get table columns type ");
	}
	LOG.info("tableName is ------------------>>"+tableName);
  
    String mysqlurl = jc.get( "javax.jdo.option.ConnectionURL" );
	if (mysqlurl == null) {
		mysqlurl = tableProperties.getProperty(HIVE_MYSQL_URL); jc.get( "javax.jdo.option.ConnectionURL" );
		if(mysqlurl == null){
		   throw new RuntimeException( "Please set " + HIVE_MYSQL_URL + " to get table columns type ");
        }
	}
    String username = jc.get( "javax.jdo.option.ConnectionUserName" );
	if (username == null) {
		username = tableProperties.getProperty(HIVE_MYSQL_USER);
		if(username == null){
		    throw new RuntimeException( "Please set " + HIVE_MYSQL_PASS + " to get table columns type ");
	    }
	}
    String password = tableProperties.getProperty(HIVE_MYSQL_PASS);
	if (password == null) {
		throw new RuntimeException( "Please set " + HIVE_MYSQL_PASS + " to get table columns type ");
    }


    // Target path's last component is also the column family name.
    final Path columnFamilyPath = new Path(hfilePath);
    final String columnFamilyName = columnFamilyPath.getName();
    final byte [] columnFamilyNameBytes = Bytes.toBytes(columnFamilyName);
    final Job job = new Job(jc);
    setCompressOutput(job, isCompressed);
    setOutputPath(job, finalOutPath);

    // Create the HFile writer
    final org.apache.hadoop.mapreduce.TaskAttemptContext tac =
      ShimLoader.getHadoopShims().newTaskAttemptContext(
          job.getConfiguration(), progressable);

    final Path outputdir = FileOutputFormat.getOutputPath(tac);
    final org.apache.hadoop.mapreduce.RecordWriter<
      ImmutableBytesWritable, KeyValue> fileWriter = getFileWriter(tac);

    // Individual columns are going to be pivoted to HBase cells,
    // and for each row, they need to be written out in order
    // of column name, so sort the column names now, creating a
    // mapping to their column position.  However, the first
    // column is interpreted as the row key.
    String columnList = tableProperties.getProperty("columns");
    String [] columnArray = columnList.split(",");
    final SortedMap<byte [], Integer> columnMap =
      new TreeMap<byte [], Integer>(Bytes.BYTES_COMPARATOR);
    int i = 0;
    for (String columnName : columnArray) {
      if (i != 0) {
        columnMap.put(Bytes.toBytes(columnName), i);
      }
      ++i;
    }
    
    final Map<String,String> columnTypes = new HashMap<String,String>();
    getColumnTypes(dbName,tableName,columnTypes,mysqlurl,username,password);
	LOG.info( "++++++++++++++++++++++++++++++++++++++++++++++++++++++"+columnTypes );
	
    return new RecordWriter() {

      @Override
      public void close(boolean abort) throws IOException {
        try {
          fileWriter.close(null);
          if (abort) {
            return;
          }
          // Move the region file(s) from the task output directory
          // to the location specified by the user.  There should
          // actually only be one (each reducer produces one HFile),
          // but we don't know what its name is.
          FileSystem fs = outputdir.getFileSystem(jc);
          fs.mkdirs(columnFamilyPath);
          Path srcDir = outputdir;
          for (;;) {
            FileStatus [] files = fs.listStatus(srcDir);
            if ((files == null) || (files.length == 0)) {
              throw new IOException("No files found in " + srcDir);
            }
            if (files.length != 1) {
              throw new IOException("Multiple files found in " + srcDir);
            }
            srcDir = files[0].getPath();
            if (srcDir.getName().equals(columnFamilyName)) {
              break;
            }
          }
          for (FileStatus regionFile : fs.listStatus(srcDir)) {
            fs.rename(
              regionFile.getPath(),
              new Path(
                columnFamilyPath,
                regionFile.getPath().getName()));
          }
          // Hive actually wants a file as task output (not a directory), so
          // replace the empty directory with an empty file to keep it happy.
          fs.delete(outputdir, true);
          fs.createNewFile(outputdir);
        } catch (InterruptedException ex) {
          throw new IOException(ex);
        }
      }

      @Override
      public void write(Writable w) throws IOException {
        // Decompose the incoming text row into fields.
        String s = ((Text) w).toString();
        String [] fields = s.split("\u0001");
        assert(fields.length <= (columnMap.size() + 1));
        // First field is the row key.
        byte [] rowKeyBytes = Bytes.toBytes(fields[0]);
        // Remaining fields are cells addressed by column name within row.
        for (Map.Entry<byte [], Integer> entry : columnMap.entrySet()) {
          byte [] columnNameBytes = entry.getKey();
          int iColumn = entry.getValue();
          String val;
          if (iColumn >= fields.length) {
            // trailing blank field
            val = "";
          } else {
            val = fields[iColumn];
            if ("\\N".equals(val)) {
              // omit nulls
              continue;
            }
          }
          
          
          byte [] valBytes = Bytes.toBytes(val);
                   
                    if(columnTypes.containsKey( Bytes.toString(columnNameBytes) )){
                  	  String type = columnTypes.get( Bytes.toString(columnNameBytes) );
                  	  if("bigint".equalsIgnoreCase( type )){
                  		  try {
          					long temp = Long.parseLong( val );
          					  valBytes = Bytes.toBytes(temp);
          				} catch( NumberFormatException e ) {
          					e.printStackTrace();
          				}
                  	  }
                  	  if("int".equalsIgnoreCase( type )){
                  		  try {
            					int  temp = Integer.parseInt( val );
            					  valBytes = Bytes.toBytes(temp);
            				} catch( NumberFormatException e ) {
            					e.printStackTrace();
            				}
                  	  }
                  	  if("double".equalsIgnoreCase( type )){
                  		  try {
                  			  double  temp = Double.parseDouble( val );
                  			  valBytes = Bytes.toBytes(temp);
                  		  } catch( NumberFormatException e ) {
                  			  e.printStackTrace();
                  		  }
                  	  }
					  if("float".equalsIgnoreCase( type )){
	                      try {
							   float  temp = Float.parseFloat( val );
							   valBytes = Bytes.toBytes(temp);
	             		  } catch( NumberFormatException e ) {
							   e.printStackTrace();
		           		  }
				  	  }
                  	  
                    }
          KeyValue kv = new KeyValue(
            rowKeyBytes,
            columnFamilyNameBytes,
            columnNameBytes,
            valBytes);
          try {

            fileWriter.write(null, kv);
          } catch (InterruptedException ex) {
            throw new IOException(ex);
          }
        }
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf jc) throws IOException {
    //delegate to the new api
    Job job = new Job(jc);
    JobContext jobContext = ShimLoader.getHadoopShims().newJobContext(job);

    checkOutputSpecs(jobContext);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<ImmutableBytesWritable, KeyValue> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
    throw new NotImplementedException("This will not be invoked");
  }
}
