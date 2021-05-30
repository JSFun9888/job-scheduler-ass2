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

public class RUOptimizer {
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
  public static String GETS_AVAIL = "GETS Avail";

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

  public RUOptimizer(){
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

      RUOptimizer ruo = new RUOptimizer();

      //perform server handshake
      ruo.handshake(ruo, bout, bin);
        
      //main loop, while there are jobs
      while(!ruo.serverReply.trim().equals(NONE)) {

        //send REDY msg
        ruo.sendMsg(REDY, bout);

        //read the reply to REDY
        ruo.readServerMsg(ruo, bin);
       
        //job capture
        ruo.jobCapture(ruo, bout, bin);

        //Exit main loop if no more jobs received "NONE"
        if(ruo.jobArr[0].trim().equals(NONE)){
          break;
        }

        //get the job info
        ruo.extractJobInfo(ruo);

        
        //RUO algo start
        ruo.findBestServer(ruo, bin, bout, br);;

        //create biggest server schedule message then
        //Schedule the job to the biggest server
        ruo.scheduleJob(ruo, bin, bout);
      } 

      //tell server to quit
      ruo.sendMsg(QUIT, bout);
      
      //read reply
      ruo.readServerMsg(ruo, bin);
      
      //quit once server acknowledes "QUIT"
      ruo.quit(ruo, dout, bout, s);
      
      
    } catch(Exception e){
      System.out.println(e);
    }
  }


  /*
        **Methods Below**
  */


  //performs the server handshake
  public void handshake(RUOptimizer ruo, BufferedOutputStream bout, BufferedInputStream bin){
      //send HELO mesg
      ruo.sendMsg(HELO, bout);

      //read the reply for HELO
      ruo.readServerMsg(ruo, bin); 

      //send Auth msg to server
      ruo.sendMsg(AUTH, bout);

      //read the reply AUTH
      ruo.readServerMsg(ruo, bin); 
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
      bout.flush();
    } catch(Exception e){
      System.out.println(e);
    }

  }

  //read and print out the server msg
  public void readServerMsg(RUOptimizer ruo, BufferedInputStream bin){
    ruo.serverReply = ruo.readMsg(new byte[buffSize], bin);
  }

  //read and store the server strings one line at a time
  public void readServerMsgDynamic(RUOptimizer ruo, BufferedReader br, int arrSize){

    //change this to a list for later
    ruo.serverStringArr = new String[arrSize];
      for(int i = 0; i < arrSize ; i++){
        try {
          ruo.serverStringArr[i] = br.readLine();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
  }

  //create a list of servers from a string array
  public void populateServerList(String[] arrOfStr, RUOptimizer ruo) {
    ruo.serverList = new ArrayList<Servers>();
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
      ruo.serverList.add(serverIndividual);
    }
  }

  //find the server that has the exact core count for job
  //or the first server with the most cores avail
  public void findCapableServer(RUOptimizer ruo){
    int lowestCores = Integer.MAX_VALUE;
    for(Servers s: ruo.serverList){
      if(ruo.currJob.core - s.cores == 0){
        lowestCores = ruo.currJob.core - s.cores;
        ruo.capableServer= s;
        break;
      } else if(ruo.currJob.core - s.cores < lowestCores){
        lowestCores = ruo.currJob.core - s.cores;
        ruo.capableServer= s;
      }
    }
  }

  //create the schedule message to send
  public void biggestServerMsg(RUOptimizer ruo){
    ruo.bigServer = "SCHD " + Integer.toString(ruo.currJob.jobID) + " " + ruo.biggestServer.serverName + " " + Integer.toString(ruo.biggestServer.serverId) + "\n";
  }

  //create a schedule msg for the 1st capable server
  public void capableServerMsg(RUOptimizer ruo) {
    ruo.capServer = "SCHD " + Integer.toString(ruo.currJob.jobID) + " " + ruo.capableServer.serverName + " " + Integer.toString(ruo.capableServer.serverId) + "\n";
  }

  //get the job information
  public void extractJobInfo(RUOptimizer ruo){
    ruo.currJob.submitTime = Integer.parseInt(ruo.jobArr[1]);
    ruo.currJob.jobID = Integer.parseInt(ruo.jobArr[2]);
    ruo.currJob.estRuntime = Integer.parseInt(ruo.jobArr[3]);
    ruo.currJob.core = Integer.parseInt(ruo.jobArr[4]);
    ruo.currJob.memory = Integer.parseInt(ruo.jobArr[5]);
    ruo.currJob.disk = Integer.parseInt(ruo.jobArr[6].trim());
  }

  //gets the next job and filters out job completed msgs
  public void jobCapture(RUOptimizer ruo, BufferedOutputStream bout, BufferedInputStream bin){
    ruo.jobArr = ruo.serverReply.split(" ");
    if(ruo.jobArr[0].equals(JCPL)){
      while(ruo.jobArr[0].equals(JCPL)){

        //send REDY msg
        ruo.sendMsg(REDY, bout);

        //read the reply to REDY
        ruo.serverReply = ruo.readMsg(new byte[buffSize], bin);
        
        ruo.jobArr = ruo.serverReply.split(" ");
      }
    }
  }

  //gets the list of servers from the server
  public void getServers(RUOptimizer ruo, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br){
    if(ruo.serverList.isEmpty()){
      ruo.sendMsg(GETSALL, bout);

      ruo.readServerMsg(ruo, bin);
      
      String[] dataArr = ruo.serverReply.split(" "); //split response into words
      ruo.sendMsg(OK, bout);

      //read the msg one line at a time
      ruo.readServerMsgDynamic(ruo, br, Integer.parseInt(dataArr[1]));
      
      String[] arrOfStr = ruo.serverStringArr; //copy the strings over into arrOfStr
      //add servers to the server list with their info
      ruo.populateServerList(arrOfStr, ruo);
    
      //System.out.println("RCVD in response to ok: " + ruo.serverReply);

      ruo.sendMsg(OK, bout);
      //get reply from server
      ruo.readServerMsg(ruo, bin);
    }
  }

  //gets capable servers from the server
  public void getsCapable(RUOptimizer ruo, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br) {
    String capable = GETS_CAPABLE + " " + ruo.currJob.coreMemDisk();
    ruo.sendMsg(capable, bout);
    
    ruo.readServerMsg(ruo, bin);
    String[] dataArr = ruo.serverReply.split(" "); //split response into words
    ruo.sendMsg(OK, bout);
    
    //read the msg one line at a time
    ruo.readServerMsgDynamic(ruo, br, Integer.parseInt(dataArr[1]));
    String[] arrOfStr = ruo.serverStringArr; //copy the strings over into arrOfStr
    //add servers to the server list with their info
    ruo.populateServerList(arrOfStr, ruo);
    
    
    ruo.sendMsg(OK, bout);
    //get reply from server
    ruo.readServerMsg(ruo, bin);
    
  }

  public void findBestServer(RUOptimizer ruo, BufferedInputStream bin, BufferedOutputStream bout, BufferedReader br){
    String available = GETS_AVAIL + " " + ruo.currJob.coreMemDisk();
    
    ruo.sendMsg(available, bout);
    ruo.readServerMsg(ruo, bin);
    
    String[] dataArr = ruo.serverReply.split(" "); //split response into words
    
    if(Integer.parseInt(dataArr[1]) > 0){
      
      ruo.sendMsg(OK, bout);
      ruo.readServerMsgDynamic(ruo, br, Integer.parseInt(dataArr[1]));

      String[] arrOfStr = ruo.serverStringArr;

      ruo.populateServerList(arrOfStr, ruo);
      

      ruo.sendMsg(OK, bout);

      ruo.readServerMsg(ruo, bin);

      ArrayList<Servers> idleList = new ArrayList<Servers>();
      ArrayList<Servers> activeList = new ArrayList<Servers>();
      ArrayList<Servers> bootingList = new ArrayList<Servers>();
      ArrayList<Servers> inactiveList = new ArrayList<Servers>();
      for(Servers s: ruo.serverList){
        if(s.cores != 0){
          
          if(s.cores >= ruo.currJob.core){
            
            if(s.state.equals("idle")) {
              idleList.add(s);
            } else if(s.state.equals("active")){
              activeList.add(s);
            } else if(s.state.equals("booting")){
              bootingList.add(s);
            } else {
              inactiveList.add(s);
            }
            
          }
          
        }
      }
      if(idleList.size() > 0){
        ruo.capableServer = idleList.get(0);
      } else if(activeList.size() > 0){
        ruo.capableServer = activeList.get(0);
      } else if(bootingList.size() > 0){
        ruo.capableServer = bootingList.get(0);
      } else if(inactiveList.size() > 0){
        ruo.capableServer = inactiveList.get(0);
      }
      
      if(ruo.capableServer == null){
        ruo.getsCapable(ruo, bin, bout, br);
        ruo.findCapableServer(ruo);
      }
      
    } else {
      
      ruo.sendMsg(OK, bout);
      ruo.readServerMsg(ruo, bin);

      ruo.getsCapable(ruo, bin, bout, br);
      
      ruo.findCapableServer(ruo);
      
    }
        
  }

  //creates and sends schedule message to server and reads reply from server
  public void scheduleJob(RUOptimizer ruo, BufferedInputStream bin, BufferedOutputStream bout){
    ruo.capableServerMsg(ruo);   
    ruo.sendMsg(ruo.capServer, bout);
    ruo.readServerMsg(ruo, bin);

    ruo.serverList = new ArrayList<Servers>();
    //reset the capable server
    ruo.capableServer = null;
  }

  //has the RUOptimizer Client quit communicating with the server
  public void quit(RUOptimizer ruo, DataOutputStream dout, BufferedOutputStream bout, Socket s){
    try{
      if(ruo.serverReply.equals(QUIT)){
        bout.close();
        dout.close();
        s.close();
      }
    } catch(Exception e){
      System.out.println(e);
    }
  }
}
