/*
COMP3100 Distributed Systems project 2
Authors: Jaime Sun
Student ID:45662398
Practical Session: Wednesday 13:00 - 14:55
*/
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


import java.io.*;
//import Servers.java;

public class AllToLargest {
  //Server Messages
  public static String HELO = "HELO\n";
  public static String AUTH = "AUTH " + System.getProperty("user.name") + "\n";
  public static String REDY = "REDY\n";
  public static String QUIT = "QUIT\n";
  public static String GETSALL = "GETS All\n";
  public static String OK = "OK\n";
  public static String NONE = "NONE";
  public static String JCPL = "JCPL";
  public static String GETS_CAPABLE = "GETS Capable";

  public ArrayList<Servers> serverList = new ArrayList<Servers>();
  public String serverReply = "";
  public Servers biggestServer = null;
  public Servers capableServer = null;
  public int coreCount = -1;
  public Jobs currJob;
  public String bigServer = "";
  public String capServer = "";
  public String[] jobArr = null;
  public String[] serverStringArr = null;
  public static int buffSize = 1000;
  public static String IP = "127.0.0.1";
  public static int PORT = 50000;

  public AllToLargest(){
    currJob = new Jobs();
  }

  public static void main (String args []){
    try{
      Socket s = new Socket(IP, PORT);
      DataInputStream din = new DataInputStream(s.getInputStream());
      DataOutputStream dout = new DataOutputStream(s.getOutputStream());
      
      BufferedOutputStream bout = new BufferedOutputStream(dout);
      BufferedInputStream bin = new BufferedInputStream(din);
      BufferedReader br = new BufferedReader(
        new InputStreamReader(bin, StandardCharsets.UTF_8));
      //System.out.println("connected");

      AllToLargest atl = new AllToLargest();

      //perform server handshake
      atl.handshake(atl, bout, bin);
        
      //main loop, while there are jobs
      while(!atl.serverReply.trim().equals(NONE)) {

        //send REDY msg
        atl.sendMsg(REDY, bout);

        //read the reply to REDY
        atl.readServerMsg(atl, bin);
       
        //job capture
        atl.jobCapture(atl, bout, bin);
       
        //System.out.println("Checking what serverReply is: ");
        //System.out.println(atl.serverReply);

        //Exit main loop if no more jobs received "NONE"
        System.out.println(atl.jobArr[0]);
        if(atl.jobArr[0].trim().equals(NONE)){
          //System.out.println("NO MORE JOBS RCVD!");
          break;
        }

        //get the job info
        atl.extractJobInfo(atl);

        //send GETS All msg to get a list of servers 
        //but only if not already done
        //atl.getServers(atl, bin, bout, br);
        
        //wk 8: send gets capable msg
        atl.findBestServer(atl, bin, bout, br);;
        
        //find biggest server if not found already
        //if(atl.biggestServer==null){
        //  atl.findBiggestServer(atl);
        //}
        //System.out.println("Checking I am finding the biggest server");
        //System.out.println(atl.bigServer);

        //get the first capable server
        //atl.findCapableServer(atl);
        //atl.capableServer = atl.serverList.get(0);

        //create biggest server schedule message then
        //Schedule the job to the biggest server
        atl.scheduleJob(atl, bin, bout);
      } 

      //tell server to quit
      atl.sendMsg(QUIT, bout);
      
      //read reply
      atl.readServerMsg(atl, bin);
      
      //quit once server acknowledes "QUIT"
      atl.quit(atl, dout, bout, s);
      
      
    } catch(Exception e){
      System.out.println(e);
    }
  }


  /*
        **Methods Below**
  */


  //performs the server handshake
  public void handshake(AllToLargest atl, BufferedOutputStream bout, BufferedInputStream bin){
      //send HELO mesg
      //dout.writeBytes(HELO);
      atl.sendMsg(HELO, bout);

      //read the reply for HELO
      atl.readServerMsg(atl, bin); 

      //send Auth msg to server
      atl.sendMsg(AUTH, bout);

      //read the reply AUTH
      atl.readServerMsg(atl, bin); 
  }

  //Method to read a byte[] msg from the server, returns the string
  public String readMsg(byte[] b, BufferedInputStream bis) {
    try {
      bis.read(b);
      String str = new String(b, StandardCharsets.UTF_8);
      return str;
    } catch (Exception e){
      System.out.println(e);
    }
    return "error";
  }
  
  //send a message to the server
  public void sendMsg(String msg, BufferedOutputStream bout) {
    try{
      bout.write(msg.getBytes());
      //System.out.println("SENT: " + msg);
      bout.flush();
    } catch(Exception e){
      System.out.println(e);
    }

  }

  //read and print out the server msg
  public void readServerMsg(AllToLargest atl, BufferedInputStream bin){
    atl.serverReply = atl.readMsg(new byte[buffSize], bin);
    //System.out.println("RCVD in response: " + atl.serverReply);
  }

  //read and store the server strings one line at a time
  public void readServerMsgDynamic(AllToLargest atl, BufferedReader br, int arrSize){

    //change this to a list for later
    atl.serverStringArr = new String[arrSize];
      for(int i = 0; i < arrSize ; i++){
        try {
          atl.serverStringArr[i] = br.readLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
  }

  //create a list of servers from a string array
  public void populateServerList(String[] arrOfStr, AllToLargest atl) {
    atl.serverList = new ArrayList<Servers>();
    for(String server: arrOfStr){
      String[] individualServer = server.split(" ");
      Servers serverIndividual = new Servers();
      serverIndividual.serverName = individualServer[0];
      serverIndividual.serverId = Integer.parseInt(individualServer[1]);
      serverIndividual.state = individualServer[2];
      serverIndividual.currStartTime = Integer.parseInt(individualServer[3]);
      serverIndividual.cores = Integer.parseInt(individualServer[4]);
      serverIndividual.mem = Integer.parseInt(individualServer[5]);
      serverIndividual.disk = Integer.parseInt(individualServer[6]);
      atl.serverList.add(serverIndividual);
    }
  }

  //determine which server is biggest
  public void findBiggestServer(AllToLargest atl){
    for(Servers serverToInspect: atl.serverList){
      if(atl.coreCount < serverToInspect.cores){
        atl.biggestServer = serverToInspect;
        atl.coreCount = serverToInspect.cores;
      }
    }
  }

  public void findCapableServer(AllToLargest atl){
    int lowestCores = Integer.MAX_VALUE;
    for(Servers s: atl.serverList){
      if(atl.currJob.core - s.cores == 0){
        lowestCores = atl.currJob.core - s.cores;
        atl.capableServer= s;
        break;
      } else if(atl.currJob.core - s.cores < lowestCores){
        lowestCores = atl.currJob.core - s.cores;
        atl.capableServer= s;
      }
    }
  }

  //create the schedule message to send
  public void biggestServerMsg(AllToLargest atl){
    atl.bigServer = "SCHD " + Integer.toString(atl.currJob.jobID) + " " + atl.biggestServer.serverName + " " + Integer.toString(atl.biggestServer.serverId) + "\n";
    //System.out.println("The biggest Server is: " + atl.bigServer);
  }

  //create a schedule msg for the 1st capable server
  public void capableServerMsg(AllToLargest atl) {
    atl.capServer = "SCHD " + Integer.toString(atl.currJob.jobID) + " " + atl.capableServer.serverName + " " + Integer.toString(atl.capableServer.serverId) + "\n";
    //System.out.println("Scheduling msg: ");
    //System.out.println(atl.capServer);
  }

  //get the job information
  public void extractJobInfo(AllToLargest atl){
    System.out.println("Checking extractjobInfo");
    System.out.println("Length of the jobArr");
    System.out.println(atl.jobArr.length);
    System.out.println(atl.jobArr[0]);
    atl.currJob.submitTime = Integer.parseInt(atl.jobArr[1]);
    System.out.println("extractjobInfo checked");
    atl.currJob.jobID = Integer.parseInt(atl.jobArr[2]);
    atl.currJob.estRuntime = Integer.parseInt(atl.jobArr[3]);
    atl.currJob.core = Integer.parseInt(atl.jobArr[4]);
    atl.currJob.memory = Integer.parseInt(atl.jobArr[5]);
    atl.currJob.disk = Integer.parseInt(atl.jobArr[6].trim());
    //System.out.println(atl.currJob.jobID);
  }

  //gets the next job and filters out job completed msgs
  public void jobCapture(AllToLargest atl, BufferedOutputStream bout, BufferedInputStream bin){
    System.out.println("Checking the job message");
    System.out.println(atl.serverReply);
    atl.jobArr = atl.serverReply.split(" ");
    System.out.println(atl.jobArr[0]);
    if(atl.jobArr[0].equals(JCPL)){
      while(atl.jobArr[0].equals(JCPL)){
        //System.out.println("We got a job completed msg!");

        //send REDY msg
        atl.sendMsg(REDY, bout);

        //read the reply to REDY
        atl.serverReply = atl.readMsg(new byte[buffSize], bin);
        //System.out.println("RCVD in response to REDY(job completed rdy): " + atl.serverReply);

        atl.jobArr = atl.serverReply.split(" ");
      }
    }
  }

  //gets the list of servers from the server
  public void getServers(AllToLargest atl, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br){
    if(atl.serverList.isEmpty()){
      atl.sendMsg(GETSALL, bout);

      atl.readServerMsg(atl, bin);
      
      String[] dataArr = atl.serverReply.split(" "); //split response into words
      atl.sendMsg(OK, bout);

      //read the msg one line at a time
      atl.readServerMsgDynamic(atl, br, Integer.parseInt(dataArr[1]));
      
      String[] arrOfStr = atl.serverStringArr; //copy the strings over into arrOfStr
      //add servers to the server list with their info
      atl.populateServerList(arrOfStr, atl);
    
      //System.out.println("RCVD in response to ok: " + atl.serverReply);

      atl.sendMsg(OK, bout);
      //get reply from server
      atl.readServerMsg(atl, bin);
    }
  }

  //gets capable servers from the server
  public void getsCapable(AllToLargest atl, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br) {
    System.out.println("1st debug point 1");
    String capable = GETS_CAPABLE + " " + atl.currJob.coreMemDisk();
    atl.sendMsg(capable, bout);
    
    atl.readServerMsg(atl, bin);
    System.out.println("1st debug point 1.1");
    System.out.println(atl.serverReply);
    String[] dataArr = atl.serverReply.split(" "); //split response into words
    System.out.println(dataArr.length);
    atl.sendMsg(OK, bout);
    System.out.println("1st debug point 1.2");
    //read the msg one line at a time
    atl.readServerMsgDynamic(atl, br, Integer.parseInt(dataArr[1]));
    System.out.println("1st debug point 1.3");
    String[] arrOfStr = atl.serverStringArr; //copy the strings over into arrOfStr
    //add servers to the server list with their info
    System.out.println("1st debug point 1.4");
    atl.populateServerList(arrOfStr, atl);
    System.out.println("1st debug point 1.5");
    //System.out.println("These are my capable servers:");
    //for(int i = 0; i < arrOfStr.length; i++){
    //  System.out.println(arrOfStr[i]);
    //}
  
    //System.out.println("RCVD in response to ok: " + atl.serverReply);
    //System.out.println("This is my first capable server: " + arrOfStr[0]);

    atl.sendMsg(OK, bout);
    //get reply from server
    atl.readServerMsg(atl, bin);
    System.out.println("1st debug point 2");
  }

  public void findBestServer(AllToLargest atl, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br){
    String available = "GETS Avail" + " " + atl.currJob.coreMemDisk();
    System.out.println(atl.currJob.jobID);
    atl.sendMsg(available, bout);

    atl.readServerMsg(atl, bin);
    System.out.println(atl.serverReply);
    String[] dataArr = atl.serverReply.split(" "); //split response into words
    System.out.println("Printing the dataArr length:");
    System.out.println(dataArr.length);
    if(Integer.parseInt(dataArr[1]) > 0){
      System.out.println("finding gets avail");
      atl.sendMsg(OK, bout);

      atl.readServerMsgDynamic(atl, br, Integer.parseInt(dataArr[1]));

      String[] arrOfStr = atl.serverStringArr;
      for(String a: arrOfStr){
        //System.out.println(a);
      }

      atl.populateServerList(arrOfStr, atl);
      System.out.println("Popoulated server list");
      //System.out.println(atl.serverList.get(0).serverName);

      atl.sendMsg(OK, bout);

      atl.readServerMsg(atl, bin);

      System.out.println("Assigning the capable server");
      ArrayList<Servers> idleList = new ArrayList<Servers>();
      ArrayList<Servers> activeList = new ArrayList<Servers>();
      ArrayList<Servers> bootingList = new ArrayList<Servers>();
      ArrayList<Servers> inactiveList = new ArrayList<Servers>();
      for(Servers s: atl.serverList){
        if(s.cores != 0){
          //System.out.println("core is not equal 0!");
          if(s.cores >= atl.currJob.core){
            //System.out.println(s.serverName);
            if(s.state.equals("idle")) {
              idleList.add(s);
            } else if(s.state.equals("active")){
              activeList.add(s);
            } else if(s.state.equals("booting")){
              bootingList.add(s);
            } else {
              inactiveList.add(s);
            }
            
            //System.out.println("Capable server is: ");
            //System.out.println(atl.capableServer.serverName);
          }
          
        }
      }
      if(idleList.size() > 0){
        atl.capableServer = idleList.get(0);
      } else if(activeList.size() > 0){
        atl.capableServer = activeList.get(0);
      } else if(bootingList.size() > 0){
        atl.capableServer = bootingList.get(0);
      } else if(inactiveList.size() > 0){
        atl.capableServer = inactiveList.get(0);
      }
      
      System.out.println("Finished assigning the capable server");
      if(atl.capableServer == null){
        //System.out.println("available was not assigned!");
        atl.getsCapable(atl, bin, bout, br);
        atl.findCapableServer(atl);
      }
      //atl.capableServer = atl.serverList.get(0);
    } else {
      System.out.println("finding gets capable instead");
      atl.sendMsg(OK, bout);
      atl.readServerMsg(atl, bin);

      atl.getsCapable(atl, bin, bout, br);
      System.out.println("1st debug point 3");
      atl.findCapableServer(atl);
      System.out.println("1st debug point 4");
    }
        
  }

  //creates and sends schedule message to server and reads reply from server
  public void scheduleJob(AllToLargest atl, BufferedInputStream bin, BufferedOutputStream bout){
    //atl.biggestServerMsg(atl);
    atl.capableServerMsg(atl);   
    atl.sendMsg(atl.capServer, bout);
    atl.readServerMsg(atl, bin);

    atl.serverList = new ArrayList<Servers>();

    atl.capableServer = null;
  }

  //has the AllToLargest Client quit communicating with the server
  public void quit(AllToLargest atl, DataOutputStream dout, BufferedOutputStream bout, Socket s){
    try{
      if(atl.serverReply.equals(QUIT)){
        bout.close();
        dout.close();
        s.close();
      }
    } catch(Exception e){
      System.out.println(e);
    }
  }
}
