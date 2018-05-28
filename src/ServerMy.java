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
    public  ConcurrentLinkedQueue<Task> getNextBatch() {
        taskInProgress = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < parallerThreadCount.get(); i++) {
            taskInProgress.add(urlTaskQueue.poll());
        }
        this.isRunning.set(true);
        return taskInProgress;
    }

    public  boolean isTaskInProgress(int taskId) {
        return taskInProgress.contains(new Task(taskId));
    }

    public  void doneTask(int taskId) {
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
    private Object cancelLockHelper = new Object();
    private Object runningLockHelper = new Object();
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private ORB ORB;

    private AtomicInteger MAX_THREAD = new AtomicInteger(0);
    private AtomicInteger actualFreeThreads = new AtomicInteger(0);
    private AtomicInteger taskIdCounter = new AtomicInteger(0);
    private AtomicInteger userIdCounter = new AtomicInteger(0);

    private ConcurrentHashMap<Integer, TasksPerUser> allTaskQueue = new ConcurrentHashMap<>();

    public Server() {
    }

    public Server(ORB orb) {
        this.ORB = orb;

        //executorService.submit(new TaskRunner());
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
                //TODO czasem leca bledy
                //synchronized (cancelLockHelper) {
                //TODO MAPa nie moze byc modyfikowana gdy jest forEach?
                ConcurrentLinkedQueue<Task> actualTaskToProcess = new ConcurrentLinkedQueue<>();
                for (Map.Entry<Integer, TasksPerUser> entry : allTaskQueue.entrySet()) {
                    TasksPerUser tasksPerUser = entry.getValue();
                    synchronized (runningLockHelper) {
                        if (tasksPerUser.canRunNextBatch(actualFreeThreads)) {
                            System.out.println("TaskRunner UserId: " + entry.getValue() + " taskowNaRaz: " + tasksPerUser.parallerThreadCount);
                            actualTaskToProcess = tasksPerUser.getNextBatch();
                            break;
                        } else {
                            System.out.println("cos nowego");
                            System.out.println("actualFreeThreads in RUN: " + actualFreeThreads + " i WTEDY: " + allTaskQueue.size());
                        }
                    }
                }

                actualTaskToProcess.forEach(task -> {
                    //TODO synchronized?
                    actualFreeThreads.decrementAndGet();
                    //TODO czy synchronicznie czy asynchro
                    //bo jak synchro to nie odpali wszystkich na raz... wtedy new ThreadDoTask...
                    System.out.println("doClientTask taskId: " + task.taskId);
                    //do JOB in new Thread
                    new Thread(() -> doTask.doClientTask(task.taskId, task.url))
                            .start();
                    //taskRunner.doClientTask(task.taskId, task.url);
                    System.out.println("DONE doClientTask taskId: " + task.taskId);
                });
            //}
            //}
        }
    }

    class DoTask {

        public void doClientTask(int taskID, String url) {
            //TODO?
            try {
                org.omg.CORBA.Object namingContextObj = ORB.resolve_initial_references("NameService");
                NamingContext namingContext = NamingContextHelper.narrow(namingContextObj);

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
        }
    }

    //endregion

    //region impl method

    @Override
    public void setResources(int cores) {
        //zwiekszenie liczby rdzeni
        int actualThreadCount = MAX_THREAD.get();
        System.out.println("setResources actualThreadCount: " + actualThreadCount + " newThreadCount: " + cores);
        System.out.println("actualFreeThreads: " + actualFreeThreads.get());
        if (cores > actualThreadCount) {
            //TODO sprawdzic czy trzeba ponownie przypisywac
            actualFreeThreads.addAndGet(cores - actualThreadCount);
            //TODO sprawdzic czy mozna wykonac taski to wszystko w watkach
        } //zmniejszenie liczby rdzeni
        else if (cores < actualThreadCount) {
            //TODO sprawdzic ujemne
            actualFreeThreads.addAndGet(cores - actualThreadCount);
            if (actualFreeThreads.get() < 0) {
                actualFreeThreads.set(0);
            }
        }
        this.MAX_THREAD = new AtomicInteger(cores);
        System.out.println("actualFreeThreads: " + actualFreeThreads.get());

        executorService.submit(new TaskRunner(new DoTask()));
    }

    @Override
    public void submit(String[] urls, int tasks, int parallelTasks, IntHolder userID) {
        //TODO synchronized ?
        userID.value = userIdCounter.incrementAndGet();
        ConcurrentLinkedQueue<Task> taskQueue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < tasks; i++) {
            Task task = new Task(taskIdCounter.incrementAndGet(), urls[i]);
            taskQueue.add(task);
        }
        TasksPerUser tasksPerUser = new TasksPerUser(tasks, parallelTasks, taskQueue);
        allTaskQueue.put(userIdCounter.get(), tasksPerUser);

        executorService.submit(new TaskRunner(new DoTask()));
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

            executorService.submit(new TaskRunner(new DoTask()));
        }
    }

    @Override
    public void cancel(int userID) {
        System.out.println("CANCEL userId: " + userID);

        synchronized (cancelLockHelper) {
            TasksPerUser tasksPerUser = allTaskQueue.get(userID);
            tasksPerUser.urlTaskQueue = new ConcurrentLinkedQueue<>(); //wyczyszczenie zadan do kolejki
            if (!tasksPerUser.isRunning.get()) {
                allTaskQueue.remove(userID);
            }
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

            //tworzenie serwera
            Server server = new Server(orb);
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference(server);
            System.out.println(orb.object_to_string(ref));

            org.omg.CORBA.Object namingContextObj = orb.resolve_initial_references("NameService");
            NamingContext nCont = NamingContextHelper.narrow(namingContextObj);

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