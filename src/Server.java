import org.omg.CORBA.IntHolder;
import org.omg.CORBA.ORB;
import org.omg.CosNaming.NameComponent;
import org.omg.CosNaming.NamingContext;
import org.omg.CosNaming.NamingContextHelper;
import org.omg.PortableServer.POA;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

class Server2 extends ServerInterfacePOA {

    AtomicInteger cores = new AtomicInteger(0);
    AtomicInteger idCounter = new AtomicInteger(0);
    List<JobSet> jobSets = Collections.synchronizedList(new LinkedList<>());
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    NamingContext namingContext;
    ORB orb;

    Server2() {
    }

    public Server2(NamingContext namingContext) {
        this.namingContext = namingContext;
    }

    public Server2(ORB orb, NamingContext nCont) {
        this.orb = orb;
        this.namingContext = nCont;
    }

    private TaskRunner getTaskRunner() {
        return new CorbaTaskRunner();
    }

    @Override
    public void setResources(int cores) {
        this.cores.set(cores);
        executorService.submit(new DispatcherRunnable(getTaskRunner()));
    }

    @Override
    public void submit(String[] urls, int tasks, int parallelTasks, IntHolder userID) {
            userID.value = idCounter.incrementAndGet();
            JobSet.Builder builder = new JobSet.Builder(userID.value);
            int counter = 1;
            for(String url : urls) {
                builder.add(url);
                if(counter == parallelTasks) {
                    counter = 0;
                    jobSets.add(builder.build());
                    builder = new JobSet.Builder(userID.value);
                }
                counter++;
            }
            executorService.submit(new DispatcherRunnable(getTaskRunner()));

    }

    AtomicInteger runningJobs = new AtomicInteger(0);
    Map<Integer, Batch> taskToBatch = new HashMap<>();
    Set<Integer> idsOfUsersWithRunningTasks  = ConcurrentHashMap.newKeySet();
    Object cancelLock = new Object();
    AtomicInteger taskIDcounter = new AtomicInteger(0);

    class DispatcherRunnable implements Runnable {

        TaskRunner taskRunner;


        public DispatcherRunnable(TaskRunner taskRunner) {
            this.taskRunner = taskRunner;
        }
        @Override
        public void run() {
            if(runningJobs.get() == cores.get()) return;
            List<JobSet> local = new LinkedList<>(jobSets);
            int availableThreads = cores.get() - runningJobs.get();
            for(JobSet jobSet : local) {
                if(jobSet.size <= availableThreads) {

                    synchronized (cancelLock) {
                        if(jobSets.contains(jobSet)) {
                            synchronized (runningLock) {
                                if(!(taskToBatch.values().stream().anyMatch(batch -> batch.userID == jobSet.userID))) {
                                    jobSets.remove(jobSet);
                                    Batch batch = new Batch(jobSet.userID,jobSet.size);
                                    idsOfUsersWithRunningTasks.add(jobSet.userID);
                                    for(String url : jobSet.urls) {
                                        runningUrls.add(url);
                                        int taskID = taskIDcounter.incrementAndGet();
                                        taskToBatch.put(taskID,batch);
                                        taskRunner.runTask(taskID,url);
                                        runningJobs.incrementAndGet();
                                    }
                                    availableThreads = availableThreads - jobSet.size;
                                }
                            }
                        }
                    }
                }
            }
        }

    }

    interface TaskRunner {
        void runTask(int taskID, String url);
    }

    class CorbaTaskRunner implements TaskRunner{

        public void runTask(int taskID, String url) {
            org.omg.CORBA.Object namingContextObj = null;
            org.omg.CORBA.Object envObj = null;
            try {
                namingContextObj = orb
                            .resolve_initial_references("NameService");
                NamingContext nCont = NamingContextHelper.narrow(namingContextObj);

                NameComponent[] path = { new NameComponent(url, "Object") };
                    envObj = nCont.resolve(path);

            } catch (Exception e) {
                throw new RuntimeException();
            }
            TaskInterface cri = TaskInterfaceHelper.narrow(envObj);
            cri.start(taskID);
        }
    }

    List<String> runningUrls = Collections.synchronizedList(new LinkedList<>());

    class Batch  {
        final int userID;
        final int size;
        int ready;

        public Batch(int userID, int size) {
            this.userID = userID;
            this.size = size;
            this.ready = 0;
        }

        void taskReady() {
            this.ready++;
        }

        boolean isDone() {
            return ready == size;
        }
    }

    Object runningLock = new Object();

    @Override
    public void done(int taskID) {
        synchronized (runningLock) {
            runningJobs.decrementAndGet();
            Batch batch = taskToBatch.get(taskID);
            batch.taskReady();
            if(batch.isDone()) {
                taskToBatch.values().removeAll(Collections.singleton(batch));
            }
        }

        executorService.submit(new DispatcherRunnable(getTaskRunner()));
    }

    @Override
    public void cancel(int userID) {
        synchronized (cancelLock) {
            jobSets.removeIf(jobSet -> jobSet.userID == userID);
        }
    }

    static class JobSet {

        int userID;
        int size;
        List<String> urls;

        public JobSet(Builder builder) {
            this.urls = builder.urls;
            this.size = builder.urls.size();
            this.userID = builder.userID;
        }

        static class Builder {
            List<String> urls = new LinkedList<>();
            int userID;

            public Builder(int userID) {
                this.userID = userID;
            }

            void add(String value) {
                urls.add(value);
            }

            JobSet build() {
                return new JobSet(this);
            }
        }
    }

}

class Start2 {

    public static void main(String[] args) {
        try {
            ORB orb = ORB.init( args, null );
            POA rootpoa = (POA)orb.resolve_initial_references( "RootPOA" );
            rootpoa.the_POAManager().activate();
            org.omg.CORBA.Object namingContextObj = orb.resolve_initial_references( "NameService" );
            NamingContext nCont = NamingContextHelper.narrow( namingContextObj );
            Server2 server = new Server2(orb,nCont);
            org.omg.CORBA.Object ref = rootpoa.servant_to_reference( server );
            System.out.println( orb.object_to_string( ref ) );

            NameComponent[] path = {
                    new NameComponent( "SERVER", "Object" )
            };

            nCont.rebind( path, ref );
            orb.run();
        }
        catch ( Exception e ) {
            throw new RuntimeException("Something went wrong...");
        }
    }
}
