import org.omg.CORBA.IntHolder;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PMO_User implements PMO_RunnableAndTestable {

	private Set<Integer> globalUniqUserID;

	private ServerInterface server;
	private PMO_AtomicCounter maxConcurrentUserTasks;
	private int concurrentTasks;
	private int allTasks;
	private int userID;
	private List<PMO_Task> tasks;
	private String[] urls;
	private long submitTime;

	// do testu wykonania cancel
	private AtomicBoolean cancelExecuted = new AtomicBoolean(false);
	private boolean cancel;
	private int tasksBeforeCancel;
	private long cancelTime;

	private PMO_AtomicCounter startedTasksCounterMax;
	
	private String username;

	// zadania da sie wykonac wg. dostepnych limitow
	private boolean feasible;

	public PMO_User( String username, int concurrentTasks, ServerInterface server, PMO_CorbaHelper corba, boolean cancel, int allTasks) {
		this(username, concurrentTasks, server, corba, cancel, allTasks, true);
	}

	public PMO_User( String username, int concurrentTasks, ServerInterface server, PMO_CorbaHelper corba, boolean cancel, int allTasks,
			boolean feasible) {
		this.username = username;
		this.server = server;
		this.concurrentTasks = concurrentTasks;
		maxConcurrentUserTasks = PMO_CountersFactory.createCommonMaxStorageCounter();
		PMO_AtomicCounter concurrentUserTasks = PMO_CountersFactory.createCounterWithMaxStorageSet();

		// liczba zadan
		this.allTasks = allTasks;
		
		// czy zadania beda mogly zostac wykonane
		this.feasible = feasible;

		PMO_Log.log("Tworzenie unikalnych url");
		urls = Stream.generate(PMO_UniqStringGenerator::get).limit(allTasks).collect(Collectors.toList())
				.toArray(new String[allTasks]);

		PMO_Log.log("Tworzenie zadan uzytkownika");
		tasks = new ArrayList<>();
		Arrays.stream(urls).forEach(url -> tasks.add(new PMO_Task(url, server, corba, username )));

		// ustawienie licznika zadan rownoczesnych
		tasks.forEach(t -> t.setConcurrentTasksCounter(concurrentUserTasks));

		// licznik uruchomionych zadan
		startedTasksCounterMax = PMO_CountersFactory.createCommonMaxStorageCounter();
		PMO_AtomicCounter startedTasksCounter = PMO_CountersFactory.createCounterWithMaxStorageSet();
		tasks.forEach(t -> t.setStartedTasksCounter(startedTasksCounter));

		// czy bedzie zlecone cancel?
		cancel = this.cancel;
		if (cancel) {
			// jesli tak, to po ilu zadaniach ma sie to stac?
			int mult = allTasks / concurrentTasks;
			tasksBeforeCancel = concurrentTasks * (1 + (new Random()).nextInt(mult - 1));
			PMO_Log.log("Uzytkownik " + username + " zleci cancel po wykonaniu " + tasksBeforeCancel + " jego zadan");
			startedTasksCounterMax.setAutoRun(tasksBeforeCancel, () -> {
				cancelTime = PMO_TimeHelper.executionTime(() -> server.cancel(userID));
				cancelExecuted.set(true);
			});
		}
	}

	public void setGlobalUniqUserID(Set<Integer> globalUniqUserID) {
		this.globalUniqUserID = globalUniqUserID;
	}

	public void setCPUUsageCounter(PMO_AtomicCounter cpuUsageCounter) {
		tasks.forEach(t -> t.setCpuUsageCounter(cpuUsageCounter));
	}

	@Override
	public void run() {
		IntHolder userIDH = new IntHolder();

		submitTime = PMO_TimeHelper.executionTime(() -> server.submit(urls, allTasks, concurrentTasks, userIDH));

		userID = userIDH.value;

		if (!globalUniqUserID.add(userID)) {
			PMO_CommonErrorLog.error("Wykryto powtorzenie userID (" + userID + ") - to blad krytyczny");
			PMO_CommonErrorLog.criticalMistake();
		}
	}

	@Override
	public boolean test() {
		if (submitTime < 0) {
			PMO_CommonErrorLog.error("Zlecenie wykonania zadan spowodowalo blad");
			return false;
		}
		if (submitTime > PMO_Consts.TASKS_SUBMITION_TIME_LIMIT) {
			PMO_CommonErrorLog.error("Przekroczono limit czasu na wprowadzenie zadan do systemu");
			PMO_CommonErrorLog.error(
					"jest " + submitTime + "ms, a powinno byc nie wiecej niz " + PMO_Consts.TASKS_SUBMITION_TIME_LIMIT);
			return false;
		} else {
			PMO_Log.log("Wprowadzenie zadania trwalo " + submitTime + " msec");
		}

		if (!feasible)
			return true;
		
		if (!cancel) {
			PMO_Log.log("User " + username + " #" + userID + " mialy zostac wykonane wszystkie");
			if (startedTasksCounterMax.get() != allTasks) {
				PMO_CommonErrorLog.error("Liczba uruchomionych zadan nie jest rowna ich ilosci");
				PMO_CommonErrorLog.error("jest " + startedTasksCounterMax.get() + ", a powinno " + allTasks);
				return false;
			} else {
				PMO_Log.log("Wszystkie zadania uzytkownika " + username + " #" + userID + " zostaly wykonane");
			}
		} else {
			if (cancelTime > PMO_Consts.TASKS_SUBMITION_TIME_LIMIT) {
				PMO_CommonErrorLog.error("Przekroczono limit czasu na wykonanie cancel");
				PMO_CommonErrorLog.error("jest " + cancelTime + "ms, a powinno byc nie wiecej niz "
						+ PMO_Consts.TASKS_SUBMITION_TIME_LIMIT);
				return false;
			}
		}

		if (maxConcurrentUserTasks.get() != concurrentTasks) {
			PMO_CommonErrorLog.error("Zadania dla " + username + " nie byly realizowane w grupach o wymaganej wielkosci");
			PMO_CommonErrorLog.error("Stwierdzono wykonanie " + maxConcurrentUserTasks.get() + " rownoczesnych zadan"
					+ ", a zlecono " + concurrentTasks);
			return false;
		} else {
			PMO_Log.log("Zadania uzytkownika " + username + " #" + userID + " realizowano w grupach po " + maxConcurrentUserTasks.get()
					+ " sztuk");
		}

		return true;
	}

}
