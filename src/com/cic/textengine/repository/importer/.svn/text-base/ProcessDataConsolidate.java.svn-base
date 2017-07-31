package com.cic.textengine.repository.importer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.cic.textengine.datadelivery.DataConsolidate;
import com.cic.textengine.datadelivery.DcmisDB;
import com.cic.textengine.datadelivery.GlobalDB;
import com.cic.textengine.repository.ItemImporter;
import com.cic.textengine.repository.config.Configurer;
import com.cic.textengine.repository.importer.exception.ImporterProcessException;
import com.cic.textengine.repository.importer.exception.ProcessDataConsolidateException;
import com.cic.textengine.repository.namenode.client.NameNodeClient;
import com.cic.textengine.repository.namenode.client.exception.NameNodeClientException;
import com.cic.textengine.repository.type.PartitionKey;

public class ProcessDataConsolidate implements ImporterProcess {

	private static Logger logger = Logger
			.getLogger(ProcessDataConsolidate.class);
	/*
	 * status = 2 means the operation is in T_CHECKPOINT table
	 * status = 8 means the operation is finished by TE
	 */
	
	private final static int OPERATION_READY = 2;
	private final static int OPERATION_FINISH = 8;
	
	private final static int CHECKPOINT_START = 0;
	private final static int CHECKPOINT_FINISH = 2;
	
	private File isolationPath = null;
	public ProcessDataConsolidate(File isolationPath) {
		this.isolationPath = isolationPath;
	}

	public void process(ItemImporterPerformanceLogger perfLogger)
			throws ImporterProcessException {

		long startDT = System.currentTimeMillis();
		long totalItemCount = 0;
		
		ArrayList<Long> operationIDList = new ArrayList<Long>();			// 记录固化操作的operationID
		ArrayList<Long> failOperationIDList = new ArrayList<Long>();		// 记录固化失败的operationID
		HashMap<Long, ConsolidateInfo> consolidateList= new HashMap<Long, ConsolidateInfo>();
		
		long timeStamp = System.currentTimeMillis();
		
		/*
		 * query the DCMIS database to obtain all the projects need to consolidate
		 * from T_OPERATION_LIST table:
		 */
		try {

			DcmisDB.createConnection();
			Connection conn = DcmisDB.getConnection();

			String sql = "select * from T_OPERATION_LIST where TYPE = ? and status = ?";
			PreparedStatement ps = conn.prepareStatement(sql);
			ps.setString(1, "CONSOLIDATE");
			ps.setInt(2, OPERATION_READY);
			ResultSet result = ps.executeQuery();
			while (result.next()) {
				long operationID = result.getLong("OPERATION_ID");
				int year = result.getInt("YEAR");
				int month = result.getInt("MONTH");
				int projectID = result.getInt("PROJECT_ID");
				
				operationIDList.add(operationID);
				ConsolidateInfo info = new ConsolidateInfo();
				info.year = year;
				info.month = month;
				info.prjID = projectID;
				consolidateList.put(operationID, info);
			}
			
			sql = "update T_OPERATION_LIST set START_TIME = ? where TYPE = ? and STATUS = ?";
			ps = conn.prepareStatement(sql);
			ps.setTimestamp(1, new Timestamp(timeStamp));
			ps.setString(2, "CONSOLIDATE");
			ps.setInt(3, OPERATION_READY);
			ps.execute();
		} catch (Exception e) {
			logger.error("DCMIS database operation failure:"
					+ e.getLocalizedMessage());
			DcmisDB.close();
			throw new ProcessDataConsolidateException("DCMIS database operation failure");
		}

		if (operationIDList.size() <= 0)
		{
			logger.info("No data consolidate operation.");
			DcmisDB.close();
			return;
		}
		
		
		HashMap<Long, String> instanceList = new HashMap<Long, String>();
		String iwmWorkflowHost = Configurer.getIwmWorkflowHost();
		int iwmWorkflowPort = Configurer.getIwmWorkflowPort();
		HashMap<Integer, ProjectInfo> projectInfoMap = new HashMap<Integer, ProjectInfo>();

		try{
			
			// load all the project info 
			
			GlobalDB.createConnection();
			Connection globalConn = GlobalDB.getConnection();
			Statement globalSt = globalConn.createStatement();
			
			String allProjectSql = "select project_id, product_mode, base4_priority from T_PROJECT where 1";
			globalSt.execute(allProjectSql);
			ResultSet projectResult = globalSt.getResultSet();
			while(projectResult.next()){
				int prjID = projectResult.getInt("PROJECT_ID");
				int proMode = projectResult.getInt("PRODUCT_MODE");
				int priority = projectResult.getInt("BASE4_PRIORITY");
				ProjectInfo info = new ProjectInfo();
				info.prjID = prjID;
				info.priority = priority;
				info.proMode = proMode;
				projectInfoMap.put(prjID, info);
			}
			
			for(Long id: consolidateList.keySet()){


				ConsolidateInfo consoInfo = consolidateList.get(id);
				int monthID = getMonthID(consoInfo.year, consoInfo.month);
				int prjID = consoInfo.prjID;
				ProjectInfo prjInfo = projectInfoMap.get(prjID);
				int proMode = prjInfo.proMode;
				int priority = prjInfo.priority;

				// check if there is already a base 4 workflow, if so record the instance id and continue without creating new one.
				String checkBase4SQL = String
						.format(
								"select * from T_PROJECT_WORKFLOW_INFO where WORKFLOW_TYPE = 'Base4Workflow' AND PROJECT_ID = %s AND MONTH_ID = %s AND WORKFLOW_STATUS_ID IN (4, 5)",
								prjID, monthID);
				globalSt.execute(checkBase4SQL);
				ResultSet rs = globalSt.getResultSet();
				if(rs.next()){
					logger.debug("There is already a base 4 workflow in running or error status. Skip creating new one");
					instanceList.put(id, rs.getString("WORKFLOW_INSTANCE_ID"));
					continue;
				}
				URL url = new URL(
						String
								.format(
										"http://%s:%s/invoke?"
												+ "objectname=com.cic:service=BpmServer&operation=startWorkflow"
												+ "&type0=java.lang.String&value0=Base4Workflow"
												+ "&type1=java.lang.String&value1=%s"
												+ "&type2=java.lang.String&value2=%s"
												+ "&type3=java.lang.String&value3=%s"
												+ "&type4=java.lang.String&value4=%s"
												+ "&type5=java.lang.String&value5="
												+ "&type6=java.lang.String&value6="
												+ "&type7=java.lang.String&value7="
												+ "&type8=java.lang.String&value8="
												+ "&type9=java.lang.String&value9="
												+ "&type10=java.lang.String&value10="
												+ "&type11=java.lang.String&value11="
												+ "&type12=java.lang.String&value12="
												+ "&type13=java.lang.String&value13=",
										iwmWorkflowHost, iwmWorkflowPort,
										prjID, monthID, proMode, priority));

				BufferedReader in = new BufferedReader(new InputStreamReader(
						url.openStream()));
				logger.debug("Initiate IWOM workflow:"+String
						.format(
								"http://%s:%s/invoke?"
										+ "objectname=com.cic:service=BpmServer&operation=startWorkflow"
										+ "&type0=java.lang.String&value0=Base4Workflow"
										+ "&type1=java.lang.String&value1=%s"
										+ "&type2=java.lang.String&value2=%s"
										+ "&type3=java.lang.String&value3=%s"
										+ "&type4=java.lang.String&value4=%s"
										+ "&type5=java.lang.String&value5="
										+ "&type6=java.lang.String&value6="
										+ "&type7=java.lang.String&value7="
										+ "&type8=java.lang.String&value8="
										+ "&type9=java.lang.String&value9="
										+ "&type10=java.lang.String&value10="
										+ "&type11=java.lang.String&value11="
										+ "&type12=java.lang.String&value12="
										+ "&type13=java.lang.String&value13=",
								iwmWorkflowHost, iwmWorkflowPort,
								prjID, monthID, proMode, priority));
				String str;
				boolean success = false;

				while ((str = in.readLine()) != null) {
					int start = str.indexOf("Base");
					if (start > 0) {
						int end = str.indexOf("return", start + 5);
						instanceList.put(id, str.substring(start, end - 2));
						success = true;
					}
				}

				in.close();
				if(!success){
					// fail to initiate the IWOM workflow
					logger.error(String.format("Fail to initial the IWOM workflow for Project[%s], month ID[%s].", prjID, monthID));
					throw new Exception(String.format("Fail to initial the IWOM workflow for Project[%s], month ID[%s].", prjID, monthID));
				}
			}
			
			globalSt.close();
			GlobalDB.close();
			
			// remove the operation id from the list
			

		} catch (Exception e){
			logger.error(e.getLocalizedMessage());
			GlobalDB.close();
			DcmisDB.close();
			throw new ProcessDataConsolidateException("Error to initiate the IWM workflow.");
		}
		
		
		/*
		 * consolidate the data for those projects.
		 */

		// query for the partitions need to consolidate
		HashMap<Long, ArrayList<Partition>> operationPartitionMap = new HashMap<Long, ArrayList<Partition>>();
		HashMap<Long, Boolean> operationSucess = new HashMap<Long, Boolean>();

		try {
			DcmisDB.createConnection();
			Connection conn = DcmisDB.getConnection();
			Statement stat = conn.createStatement();
			
			for (int i = 0; i < operationIDList.size(); i++) {
				long operationID = operationIDList.get(i);
				ArrayList<Partition> partitionList = new ArrayList<Partition>();
				String sql = String
						.format(
								"select * from T_CHECKPOINT where OPERATION_ID = %s and status = %s",
								operationID, CHECKPOINT_START);
				stat.execute(sql);
				ResultSet result = stat.getResultSet();
				
				while (result.next()) {
					String source = result.getString("source");
					String siteid = result.getString("site_id");
					String forumid = result.getString("forum_id").trim();
					int year = result.getInt("year");
					int month = result.getInt("month");
					Partition par = new Partition(source, siteid, forumid,
							year, month);
					partitionList.add(par);
				}
				
				sql = "update T_CHECKPOINT set start_time = ? where OPERATION_ID = ? and status = ?";
				PreparedStatement ps = conn.prepareStatement(sql);
				ps.setTimestamp(1, new Timestamp(timeStamp));
				ps.setLong(2, operationID);
				ps.setInt(3, CHECKPOINT_START);
				ps.execute();

				operationPartitionMap.put(operationID, partitionList);
			}
			
			DcmisDB.close();
		} catch (Exception e) {
			logger.error("Error query T_CHECKPOINT table for partition info: "+e.getLocalizedMessage());
			throw new ProcessDataConsolidateException(e);
		}
		
		// generate the consolidation log
		
		try {
			generateConsolidateLog(operationPartitionMap);
		} catch (IOException e1) {
			logger.error("Error in log the consolidate operation:"+e1.getLocalizedMessage());
			logger.error("Error in logging the following partition key:");
			for(long id: operationPartitionMap.keySet()) {
				ArrayList<Partition> parList = operationPartitionMap.get(id);
				for(Partition par: parList) {
					PartitionKey parkey = new PartitionKey(par.year, par.month, par.source+par.siteid, par.forumid);
					logger.error(parkey.generateStringKey());
				}
			}			
		}

		// consolidate those projects

		String nnDaemonAddr = Configurer.getNNDaemonHost();
		int nnDaemonPort = Configurer.getNNDaemonPort();
		DataConsolidate dataConsolidate = new DataConsolidate(nnDaemonAddr,
				nnDaemonPort);
		
		// clean Name Node cache first!
		NameNodeClient nnClient = new NameNodeClient(nnDaemonAddr, nnDaemonPort);
		try {
			nnClient.cleanCache();
			logger.info("Success clean name node cache.");
		} catch (NameNodeClientException e1) {
			logger.error("Fail to clean name node cache, because:" +e1.getLocalizedMessage());
			throw new ProcessDataConsolidateException(e1);
		}

		for (int i = 0; i < operationIDList.size(); i++) {
			long operationID = operationIDList.get(i);
			ArrayList<Partition> partitionList = operationPartitionMap.get(operationID);
			for (int j = 0; j < partitionList.size(); j++) {
				Partition par = partitionList.get(j);
				try {
					totalItemCount += dataConsolidate.consolidate(par.year, par.month,
							par.source+par.siteid, par.forumid);
				} catch (Exception e) {
					logger
							.error(String
									.format(
											"Error in consolidate the partition:[y:%s, m:%s, s:%s, f:%s]",
											par.year, par.month, par.source
													+ par.siteid, par.forumid));
					logger.error(e.getLocalizedMessage());
					operationSucess.put(operationID, new Boolean(false));
					break;
				}
			}
			
			if (!operationSucess.keySet().contains(operationID)) {
				operationSucess.put(operationID, new Boolean(true));
			}
		}
		
		dataConsolidate.close();
		
		Connection conn = null;
		timeStamp = System.currentTimeMillis();
		try{
			DcmisDB.createConnection();
			conn = DcmisDB.getConnection();
		} catch (Exception e)
		{
			logger.error("Error connect to DCMIS db: "+e.getLocalizedMessage());
			throw new ProcessDataConsolidateException(e);
		}
		
		// update the T_CHECKPOINT table
		
		String sql = "update T_CHECKPOINT set FINISH_TIME= ?, STATUS= ? where OPERATION_ID = ?";
		for(int i=0; i<operationIDList.size(); i++)
		{
			long operationID = operationIDList.get(i);
			if(operationSucess.get(operationID))
			{
				try {
					PreparedStatement ps = conn.prepareStatement(sql);
					ps.setTimestamp(1, new Timestamp(timeStamp));
					ps.setInt(2, CHECKPOINT_FINISH);
					ps.setLong(3, operationID);
					ps.execute();
				} catch (SQLException e) {
					logger.error("Error update the T_CHECKPOINT table: "+e.getLocalizedMessage());
					throw new ProcessDataConsolidateException(e);
				}
			}
		}
		
		// call the leoworkflow api and update the T_OPERATION_LIST table
		
		sql = "update T_OPERATION_LIST set FINISH_TIME= ?, STATUS = ? where OPERATION_ID = ?";
		for (int i = 0; i < operationIDList.size(); i++) {
			long operationID = operationIDList.get(i);
			
			if (operationSucess.get(operationID)) {
				
				// 先通知workflow，将任务状态设置为完成，然后再更新T_OPERATION_LIST；
				// 避免出现更新operation list后，调用workflow出错，无法重新固化
				String instance = instanceList.get(operationID);
				if(instance != null) {
					String callback = String.format("http://%s:%s/invoke?" +
							"objectname=com.cic:service=BpmServer&operation=notifyExecutionStatus" +
							"&type0=java.lang.String" +
							"&value0=%s", iwmWorkflowHost, iwmWorkflowPort, instance)+"&type1=java.lang.String" +
							"&value1=TE%20Consolidate" +
							"&type2=java.lang.String&value2=0";
					try{
						URL url = new URL(callback);
						logger.debug("Call back IWOM workflow to send the success signal. "+callback);
						url.openStream().close();
					} catch(Exception e){
						logger.error("Error to call back the IWM workflow.");
						logger.error(e.getLocalizedMessage());
						throw new ProcessDataConsolidateException(e);
					}
				}
				
				try{
				PreparedStatement ps = conn.prepareStatement(sql);
				ps.setTimestamp(1, new Timestamp(timeStamp));
				ps.setInt(2, OPERATION_FINISH);
				ps.setLong(3, operationID);
				ps.execute();
				} catch(SQLException e){
					logger.error("Error to call back the IWM workflow.");
					logger.error(e.getLocalizedMessage());
					throw new ProcessDataConsolidateException(e);
				}
				
			} else {
				failOperationIDList.add(operationID);				
				String instance = instanceList.get(operationID);
				if(instance != null){
					String callback = String.format("http://%s:%s/invoke?" +
							"objectname=com.cic:service=BpmServer&operation=notifyExecutionStatus" +
							"&type0=java.lang.String" +
							"&value0=%s", iwmWorkflowHost, iwmWorkflowPort, instance)+"&type1=java.lang.String" +
							"&value1=TE%20Delivery" +
							"&type2=java.lang.String&value2=1";
					try{
						URL url = new URL(callback);
						url.openStream().close();
					} catch(Exception e){
						logger.error("Error to call back the IWM workflow.");
						logger.error(e.getLocalizedMessage());
						throw new ProcessDataConsolidateException(e);
					}
				}
				
			}
		}

		try {
			perfLogger.logItemSolidifyPerformance(totalItemCount, startDT);
		} catch (IOException e) {
			logger.error("Error log the performance of data consolidate process.");
			logger.error(e.getLocalizedMessage());
		}
		
		DcmisDB.close();
		
		if (failOperationIDList.size() > 0) {
			throw new ProcessDataConsolidateException(
					"Fail to consolidate the following operation:"
							+ failOperationIDList.toString());
		}
	}
	
	private void generateConsolidateLog(HashMap<Long, ArrayList<Partition>> partitionMap) throws IOException {
		String logFile = "TE_Item_Consolidate_"+System.currentTimeMillis()+".log";
		FileWriter fw = new FileWriter(isolationPath.getAbsolutePath()+File.separator+logFile);
		PrintWriter pw = new PrintWriter(fw);
		for(long id: partitionMap.keySet()) {
			ArrayList<Partition> parList = partitionMap.get(id);
			for(Partition par: parList) {
				PartitionKey parkey = new PartitionKey(par.year, par.month, par.source+par.siteid, par.forumid);
				pw.println(parkey.generateStringKey());
			}
		}
		pw.close();
		fw.close();
	}

	static class Partition {
		String source = null;
		String siteid = null;
		String forumid = null;
		int year = 0;
		int month = 0;

		public Partition(String source, String siteid, String forumid,
				int year, int month) {
			this.source = source;
			this.siteid = siteid;
			this.forumid = forumid;
			this.year = year;
			this.month = month;
		}
	}
	
	// Test code
	public static void main(String[] args)
	{
		try {
			Configurer.config(ItemImporter.ITEM_IMPORTER_PROPERTIES);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ProcessDataConsolidate process = new ProcessDataConsolidate(new File("/Users/Joe/Isolation"));
		
		while(true){
			try {
				process.process(null);
			} catch (ImporterProcessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(1000*60);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private int getMonthID(int year, int month){
		return (year-2004)*12 + month;
	}
}

class ProjectInfo{
	int prjID;
	int priority;
	int proMode;
	String notes;
}

class ConsolidateInfo{
	int prjID;
	int year;
	int month;
}