import org.omg.CORBA.IntHolder;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NameComponent;
import org.omg.CosNaming.NamingContext;
import org.omg.CosNaming.NamingContextHelper;
import org.omg.PortableServer.POA;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class Task {
    Integer taskId;
    String url;

    public Task(int taskId) {
        this.taskId = taskId;
    }

    public Task(Integer taskId, String url) {
        this.taskId = taskId;
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Task task = (Task) o;

        return taskId != null ? taskId.equals(task.taskId) : task.taskId == null;
    }

    @Override
    public int hashCode() {
        return taskId != null ? taskId.hashCode() : 0;
    }
}

class TasksPerUser {
    AtomicInteger taskCount = new AtomicInteger(); //ilosc wszystkich taskow
    AtomicInteger parallerThreadCount = new AtomicInteger(); //ilosc taskow na raz
    AtomicBoolean isRunning = new AtomicBoolean(false); //czy zadania dla usera sa wykonywane
    //zadania wykonane beda z listy usuwane
    ConcurrentLinkedQueue<Task> urlTaskQueue = new ConcurrentLinkedQueue<>(); //LISTA z taskami uporzadkowana od czasu wejscia
    //TODO jakis czas?
    ConcurrentLinkedQueue<Task> taskInProgress = new ConcurrentLinkedQueue<>(); //LISTA aktualnie wykonujacych sie taskow.

    public TasksPerUser(int taskCount, int parallerThreadCount, ConcurrentLinkedQueue<Task> urlTaskQueue) {
        this.taskCount = new AtomicInteger(taskCount);
        this.parallerThreadCount = new AtomicInteger(parallerThreadCount);
        this.urlTaskQueue = urlTaskQueue;
    }

    public synchronized boolean canRunNextBatch(AtomicInteger actualFreeThreads) {
        if (isRunning.get() || parallerThreadCount.get() > actualFreeThreads.get() || urlTaskQueue.isEmpty()) {
            return false;
        }
        return true;
    }

    //liczba zadań będzie podzielna przez liczbę zadań, które mają być jednocześnie realizowane.
    public ConcurrentLinkedQueue<Task> getNextBatch() {
        taskInProgress = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < parallerThreadCount.get(); i++) {
            taskInProgress.add(urlTaskQueue.poll());
        }
        this.isRunning.set(true);
        return taskInProgress;
    }

    public boolean isTaskInProgress(int taskId) {
        return taskInProgress.contains(new Task(taskId));
    }

    public void doneTask(int taskId) {
        taskInProgress.remove(new Task(taskId));
        if (taskInProgress.isEmpty()) {
            this.isRunning.set(false);
        }
    }

    public boolean canRemove() {
        if (!this.isRunning.get() && urlTaskQueue.isEmpty()) {
            return true;
        }
        return false;
    }

}

///MAIN CLASS
class Server extends ServerInterfacePOA {
    private Object runningLockHelper = new Object();
    private Object submitLockHelper = new Object();
    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private ORB ORB;
    NamingContext namingContext;

    private AtomicInteger MAX_THREAD = new AtomicInteger(0);
    private AtomicInteger actualFreeThreads = new AtomicInteger(0);
    private AtomicInteger minFreeThreadsPerTask = new AtomicInteger(100); //100 -wartosc ponad stan poczatkowa.

    private AtomicInteger taskIdCounter = new AtomicInteger(0);
    private AtomicInteger userIdCounter = new AtomicInteger(0);

    private ConcurrentHashMap<Integer, TasksPerUser> allTaskQueue = new ConcurrentHashMap<>();

    public Server() {
    }

    public Server(NamingContext namingContext) {
        this.namingContext = namingContext;
    }

    public Server(ORB orb) {
        this.ORB = orb;

        //executor.submit(new TaskRunner());
    }

    //region method

    class TaskRunner implements Runnable {

        DoTask doTask;

        public TaskRunner(DoTask doTask) {
            this.doTask = doTask;
        }

        @Override
        public void run() {
            //while (true) {
            //zwroc od razu gdy na pewno za mala liczba watkow
            if (actualFreeThreads.get() < minFreeThreadsPerTask.get()) return;

            synchronized (submitLockHelper) {
                ConcurrentLinkedQueue<Task> actualTaskToProcess = new ConcurrentLinkedQueue<>();
                for (Map.Entry<Integer, TasksPerUser> entry : allTaskQueue.entrySet()) {
                    TasksPerUser tasksPerUser = entry.getValue();
                    synchronized (runningLockHelper) {
                        if (tasksPerUser.canRunNextBatch(actualFreeThreads)) {
                            actualTaskToProcess = tasksPerUser.getNextBatch();
                            break;
                        }
                    }
                }

                actualTaskToProcess.forEach(task -> {
                    actualFreeThreads.decrementAndGet();
                    //do JOB in new Thread
                    doTask.doClientTask(task.taskId, task.url);
                });
            }
            //}
        }
    }

    class DoTask {

        public void doClientTask(int taskID, String url) {
            new Thread(() -> {
                try {
                    NameComponent[] path = {
                            new NameComponent(url, "Object")
                    };
                    org.omg.CORBA.Object envObj = namingContext.resolve(path);
                    //TASK
                    TaskInterface taskInterface = TaskInterfaceHelper.narrow(envObj);
                    taskInterface.start(taskID);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }).start();
        }
    }

    //endregion

    //region impl method

    @Override
    public void setResources(int cores) {
        int actualThreadCount = MAX_THREAD.get();
        //zwiekszenie liczby rdzeni
        if (cores > actualThreadCount) {
            actualFreeThreads.addAndGet(cores - actualThreadCount);
        } //zmniejszenie liczby rdzeni
        else if (cores < actualThreadCount) {
            actualFreeThreads.addAndGet(cores - actualThreadCount);
            //dodatkowe zabezpieczenie
            if (actualFreeThreads.get() < 0) {
                actualFreeThreads.set(0);
            }
        }
        this.MAX_THREAD = new AtomicInteger(cores);

        executor.submit(new TaskRunner(new DoTask()));
    }

    @Override
    public void submit(String[] urls, int tasks, int parallelTasks, IntHolder userID) {
        synchronized (submitLockHelper) {
            if (parallelTasks < minFreeThreadsPerTask.get()) {
                minFreeThreadsPerTask.set(parallelTasks);
            }

            userID.value = userIdCounter.incrementAndGet();
            ConcurrentLinkedQueue<Task> taskQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < tasks; i++) {
                Task task = new Task(taskIdCounter.incrementAndGet(), urls[i]);
                taskQueue.add(task);
            }
            TasksPerUser tasksPerUser = new TasksPerUser(tasks, parallelTasks, taskQueue);
            allTaskQueue.put(userIdCounter.get(), tasksPerUser);
        }

        executor.submit(new TaskRunner(new DoTask()));
    }

    @Override
    public void done(int taskID) {
        synchronized (runningLockHelper) {
            System.out.println("done taskId: " + taskID);

            for (Iterator<Integer> itr = allTaskQueue.keySet().iterator(); itr.hasNext(); ) {
                Integer userId = itr.next();
                TasksPerUser tasksPerUser = allTaskQueue.get(userId);
                if (tasksPerUser.isTaskInProgress(taskID)) {
                    tasksPerUser.doneTask(taskID);
                    actualFreeThreads.incrementAndGet();
                    if (tasksPerUser.canRemove()) {
                        itr.remove();
                    }
                    break;
                }
            }

            executor.submit(new TaskRunner(new DoTask()));
        }
    }

    @Override
    public void cancel(int userID) {
        TasksPerUser tasksPerUser = allTaskQueue.get(userID);
        tasksPerUser.urlTaskQueue = new ConcurrentLinkedQueue<>(); //wyczyszczenie zadan do kolejki
        if (!tasksPerUser.isRunning.get()) {
            allTaskQueue.remove(userID);
        }
    }

    //endregion
}


class Start {

    public static void main(String[] args) {
        try {
            ORB orb = ORB.init(args, null);
            POA rootpoa = (POA) orb.resolve_initial_references("RootPOA");
            rootpoa.the_POAManager().activate();

            org.omg.CORBA.Object namingContextObj = orb.resolve_initial_references("NameService");
            NamingContext nCont = NamingContextHelper.narrow(namingContextObj);

            //tworzenie serwera
            Server server = new Server(nCont);
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference(server);
            System.out.println(orb.object_to_string(ref));

            //rejestracja pod nazwa
            NameComponent[] path = {
                    new NameComponent("SERVER", "Object")
            };

            nCont.rebind(path, ref);
            orb.run();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}