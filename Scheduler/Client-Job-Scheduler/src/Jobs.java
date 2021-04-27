public class Jobs {
    public int submitTime;
    public int jobID;
    public int estRuntime;
    public int core;
    public int memory;
    public int disk;

    public Jobs(){
        
    }

    public String coreMemDisk(){
        return Integer.toString(this.core) + " " + Integer.toString(this.memory) + " " + Integer.toString(this.disk) + "\n";
    }
}
