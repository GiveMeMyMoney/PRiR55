
public class PMO_Task extends TaskInterfacePOA {
	final private String url;
	private PMO_AtomicCounter startedTasksCounter;
	private PMO_AtomicCounter concurrentTasksCounter;
	private PMO_AtomicCounter cpuUsageCounter;
	private String username;

	private ServerInterface server;
	private long sleepTime;

	public void setCpuUsageCounter(PMO_AtomicCounter cpuUsageCounter) {
		this.cpuUsageCounter = cpuUsageCounter;
	}

	public void setConcurrentTasksCounter(PMO_AtomicCounter concurrentTasksCounter) {
		this.concurrentTasksCounter = concurrentTasksCounter;
		sleepTime = (long) (PMO_Consts.TASK_SLEEP_TIME_MIN + Math.random() * PMO_Consts.TASK_SLEEP_TIME_DELTA);
	}

	public PMO_Task(String url, ServerInterface server, PMO_CorbaHelper corba, String username ) {
		this.url = url;
		this.server = server;
		this.username = username;
		
		PMO_Log.log( "Rejestracja serwisu " + url + " dla usera " + username );

		try {
			corba.rebind( url, corba.getRootPOA().servant_to_reference(this) );			
		} catch ( Exception e ) {
			PMO_CommonErrorLog.error( "Nie udalo sie wygenerowac servanta");
			PMO_CommonErrorLog.criticalMistake();
		}
		PMO_Log.log( "Rejestracja serwisu " + url + " gotowe dla usera " + username );
	}

	public String getURL() {
		return url;
	}

	@Override
	public void start(int taskID) {
		startedTasksCounter.incAndStoreMax();
		concurrentTasksCounter.incAndStoreMax();
		cpuUsageCounter.incAndStoreMax();
		PMO_Log.log("User " + username + " Task #" + taskID + " start");

		PMO_TimeHelper.sleep(sleepTime);
		cpuUsageCounter.dec();
		concurrentTasksCounter.dec();

		server.done(taskID);
		PMO_Log.log("User " + username + " Task #" + taskID + " end");
	}

	public void setStartedTasksCounter(PMO_AtomicCounter startedTasksCounter) {
		this.startedTasksCounter = startedTasksCounter;
	}

}
