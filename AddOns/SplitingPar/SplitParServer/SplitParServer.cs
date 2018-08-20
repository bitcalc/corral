using CommonLib;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SplitParServer
{
    public class SplitParServer
    {
        public static Dictionary<string, Utils.CurrentState> ClientStates = new Dictionary<string, Utils.CurrentState>();
        public static List<BplTask> BplTasks = new List<BplTask>(); 
        static Dictionary<string, int> socketMap = new Dictionary<string, int>();
        static SplitParConfig config;
        static List<Socket> clients = new List<Socket>(); 
        
        static int numberOfClients = 1;
        static bool testWithoutServer = false;
        static bool testWithoutCorral = false;
        static bool useLocalMachine = false;
        static bool started = false;

        public enum Outcome
        {
            Correct,
            Errors,
            TimedOut,
            OutOfResource,
            OutOfMemory,
            Inconclusive,
            ReachedBound
        }

        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;

            #region for testing purposes
            for (int i = 0; i < args.Length; ++i)
                if (args[i].Equals(Utils.NoServer))
                    testWithoutServer = true;
                else if (args[i].Equals(Utils.NoCorral))
                    testWithoutCorral = true;
                else if (args[i].Equals(Utils.UseLocal))
                    useLocalMachine = true;
            #endregion

            config = Utils.LoadConfig(args[0]);
            LogWithAddress.init(System.IO.Path.Combine(config.root, Utils.RunDir));
            ServerController();
            LogWithAddress.Close();
            Console.ReadKey();
        }

        static void ServerController()
        {
            numberOfClients = config.RemoteRoots.Count;
            InstallingClients();
            SpawnClients();

            string localIP = Utils.LocalIP();
            if (localIP != null)
            {
                if (useLocalMachine)
                {
                    // connect local client
                    ConnectClient(localIP);
                    socketMap[config.root] = clients.Count - 1;
                }
                // connect remote clients
                foreach (var client in config.RemoteRoots)
                {
                    string tmp = Utils.GetRemoteMachineName(client.value);
                    ConnectClient(tmp);
                    socketMap[client.value] = clients.Count - 1;
                    LogWithAddress.WriteLine(string.Format("Machine name: {0}", tmp));
                }

                if (!started)
                {
                    started = true;
                    if (useLocalMachine)
                    {
                        SendFirstMsgToLocalClient();
                    }
                    else
                    {
                        SendFirstMsgToRemoteClient();
                    }                    
                }
            }
            else
                new Exception(string.Format("Cannot get local IP"));

            MonitoringClients();
        }


        static void InstallingClients()
        {
            var force = true;
            LogWithAddress.WriteLine(string.Format("Checking self installation"));
            try
            {
                Installer.CheckSelfInstall(config);
            }
            catch (Exception e)
            {
                LogWithAddress.WriteLine(string.Format("{0}", e.Message));
                return;
            } 

            // local machine
            if (useLocalMachine)
                ClientStates[config.root] = Utils.CurrentState.AVAIL;

            // Do remote installation
            LogWithAddress.WriteLine(string.Format("Doing remote installation"));
            for (int i = 0; i < config.RemoteRoots.Count; ++i)
            {                
                ClientStates[config.RemoteRoots[i].value] = Utils.CurrentState.AVAIL;
                LogWithAddress.WriteLine(string.Format("Installing {0}", config.RemoteRoots[i].value));
                Installer.RemoteInstall(config.root, config.RemoteRoots[i].value, config.Utils.Select(u => u.dir).Distinct(), force, config.BoogieFiles);
            }
            LogWithAddress.WriteLine(string.Format("Installation Finished."));
        }

        static void SpawnClients()
        {
            var threads = new List<Thread>();
            var workers = new List<Worker>();

            var starttime = DateTime.Now;
            Console.WriteLine("Spawning clients");

            
            var configDir = System.IO.Path.Combine(config.root, Utils.RunDir, Utils.ClientConfig);
            if (useLocalMachine)
            {
                // spawn client on own machine 
                config.DumpClientConfig(config.root, configDir, config.BoogieFiles);
                var w0 = new Worker(config.root, false, configDir);
                workers.Add(w0);
                threads.Add(new Thread(new ThreadStart(w0.Run)));
            }

            // spawn client on remote machines
            for (int i = 0; i < config.RemoteRoots.Count; ++i)
            {
                string clientRoot = config.RemoteRoots[i].value;
                configDir = System.IO.Path.Combine(clientRoot, Utils.RunDir, Utils.ClientConfig);
                config.DumpClientConfig(clientRoot, configDir, config.BoogieFiles);
                var w1 = new Worker(clientRoot, true, configDir);
                threads.Add(new Thread(new ThreadStart(w1.Run)));
                workers.Add(w1);
            }
            // start threads
            threads.ForEach(t => t.Start());
        }

        static void ConnectClient(string hostName)
        {
            try
            {
                IPHostEntry ipHostInfo = Dns.Resolve(hostName);
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, Utils.ServerPort);

                while (true)
                {
                    Socket listener = new Socket(AddressFamily.InterNetwork,
                        SocketType.Stream, ProtocolType.Tcp);
                    try
                    {
                        listener.Connect(remoteEP);

                        LogWithAddress.WriteLine(string.Format("Socket connected {0}", listener.RemoteEndPoint.ToString()));

                        byte[] data = new byte[Utils.MsgSize];
                        int receivedDataLength = listener.Receive(data); //Wait for the data
                        string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                        LogWithAddress.WriteLine(string.Format("{0}", stringData)); //Write the data on the screen

                        // reply the client
                        listener.Send(Utils.EncodeStr("Welcome " + listener.RemoteEndPoint.ToString()));

                        clients.Add(listener);
                        break;
                    }
                    catch (SocketException se)
                    {
                        LogWithAddress.WriteLine(string.Format("Not connected yet."));
                        Thread.Sleep(Utils.SleepTime);
                    }
                    catch (Exception e)
                    {
                        LogWithAddress.WriteLine(string.Format("Unexpected exception : {0}", e.ToString()));
                    }
                }
            }
            catch
            {
                LogWithAddress.WriteLine(string.Format("Cannot connect to Server"));
            }

        }

        static void CreateConnection()
        {
            LogWithAddress.WriteLine(string.Format("Set up connections"));
            IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
            IPAddress ipAddress = ipHostInfo.AddressList[0];
            IPEndPoint localEndPoint = new IPEndPoint(ipAddress, Utils.ServerPort);


            Socket listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                listener.Bind(localEndPoint);
                listener.Listen(10);

                while (clients.Count < numberOfClients)
                {
                    LogWithAddress.WriteLine(string.Format("Waiting for a connection..."));
                    clients.Add(listener.Accept());
                    LogWithAddress.WriteLine(string.Format("Connected"));
                    clients.ElementAt(clients.Count - 1).Send(Utils.EncodeStr("Hello " + clients.ElementAt(clients.Count - 1).RemoteEndPoint.ToString()));

                    // wait for the reply message
                    byte[] data = new byte[Utils.MsgSize];
                    int receivedDataLength = clients.ElementAt(clients.Count - 1).Receive(data); //Wait for the data
                    string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received

                    LogWithAddress.WriteLine(string.Format("{0}", stringData)); //Write the data on the screen
                }
            }
            catch
            {
                LogWithAddress.WriteLine(string.Format("Error"));
            }
        }



        static void MonitoringClients()
        {
            var threads = new List<Thread>();
            var workers = new List<ServerListener>();

            var starttime = DateTime.Now;
            LogWithAddress.WriteLine(string.Format("Monitoring clients"));

            for (int i = 0; i < clients.Count; ++i)
            {
                if (useLocalMachine)
                {
                    if (i == 0)
                    {
                        var listener = new ServerListener(clients[i], config.root);
                        threads.Add(new Thread(new ThreadStart(listener.Listen)));
                        workers.Add(listener);
                    }
                    else
                    {
                        var listener = new ServerListener(clients[i], config.RemoteRoots[i - 1].value);
                        threads.Add(new Thread(new ThreadStart(listener.Listen)));
                        workers.Add(listener);

                    }
                }
                else
                {
                    var listener = new ServerListener(clients[i], config.RemoteRoots[i].value);
                    threads.Add(new Thread(new ThreadStart(listener.Listen)));
                    workers.Add(listener);
                }
            }

            //threads.Add(new Thread(new ThreadStart(TaskDelivery)));
            // start threads & join
            threads.ForEach(t => t.Start());
            threads.ForEach(t => t.Join());

            Console.WriteLine("Time taken = {0} seconds", (DateTime.Now - starttime).TotalSeconds.ToString("F2"));
        }

        static void SendFirstMsgToLocalClient()
        {
            lock (ClientStates)
            {
                ClientStates[config.root] = Utils.CurrentState.BUSY;
            }

            // local client is working
            clients[0].Send(Utils.EncodeStr(Utils.StartMsg));

        }

        static void SendFirstMsgToRemoteClient()
        {
            lock (ClientStates)
            {
                ClientStates[config.RemoteRoots[config.RemoteRoots.Count - 1].value] = Utils.CurrentState.BUSY;
            }

            clients[clients.Count - 1].Send(Utils.EncodeStr(Utils.StartMsg)); 
        }

        static void TransferFiles(string folderDir, string folder, string remoteFolder)
        {
            LogWithAddress.WriteLine(string.Format("Copying folder {0} to {1}", folder, remoteFolder));
            var files = System.IO.Directory.GetFiles(System.IO.Path.Combine(folderDir, folder));
            foreach (var file in files)
            {
                var remotefile = System.IO.Path.Combine(remoteFolder, folder, System.IO.Path.GetFileName(file));
                if (!System.IO.File.Exists(remotefile))
                {
                    System.IO.Directory.CreateDirectory(System.IO.Path.Combine(remoteFolder, folder)); 
                    System.IO.File.Copy(file, remotefile, true);
                    File.SetAttributes(remotefile, FileAttributes.Normal);
                }
            }
        }

        static bool PingClient(int clientID)
        { 
            clients.ElementAt(clientID).Send(Utils.EncodeStr(Utils.PingMsg));
        
            // wait for the reply message
            byte[] data = new byte[1024];
            int receivedDataLength = clients.ElementAt(clientID).Receive(data); //Wait for the data
            string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
            if (stringData.Equals(Utils.ReadyMsg))
                return true;
            else
                return false;
        }

        static Outcome ListenToCompletion(int clientID)
        {
            byte[] data = new byte[1024];
            int receivedDataLength = clients.ElementAt(clientID).Receive(data); //Wait for the data
            string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
            if (stringData.Contains(Utils.CorrectMsg))
                return Outcome.Correct;
            else if (stringData.Contains(Utils.ErrorMsg))
                return Outcome.Errors;
            else if (stringData.Contains(Utils.ReachedBoundMsg))
                return Outcome.ReachedBound;
            else if (stringData.Contains(Utils.TimedoutMsg))
                return Outcome.TimedOut;
            else if (stringData.Contains(Utils.OutOfMemoryMsg))
                return Outcome.OutOfMemory;
            else 
                return Outcome.OutOfResource;
        } 
         
        public static void TaskDelivery()
        {
            while (true)
            {
                int taskCount = 0;
                lock (BplTasks)
                {
                    taskCount = BplTasks.Count;
                } 
                if (taskCount > 0)
                {
                    Dictionary<string, Utils.CurrentState> localClientStates;
                    lock (ClientStates)
                    {                        
                        localClientStates = new Dictionary<string, Utils.CurrentState>(ClientStates);
                    }
                    foreach(var client in localClientStates)
                        if (client.Value == Utils.CurrentState.AVAIL && 
                            !BplTasks[0].author.Equals(client.Key))
                        {
                            lock (BplTasks)
                            {                                
                                BplTask topTask = BplTasks[0];
                                string callTreeDir = System.IO.Path.Combine(topTask.author, Utils.RunDir, topTask.callTreeDir);                                
                                // let him do the first task
                                if (System.IO.File.Exists(callTreeDir))
                                {
                                    LogWithAddress.WriteLine(string.Format("Transfer {0} to {1}", topTask.ToString(), client.Key));

                                    // pick the task from a client
                                    // if localClient is the one

                                    if (client.Key.Equals(config.root))
                                    {
                                        string dir = System.IO.Path.Combine(config.root, Utils.RunDir, System.IO.Path.GetFileName(callTreeDir));
                                        if (System.IO.File.Exists(dir))
                                            System.IO.File.Delete(dir);
                                        System.IO.File.Move(callTreeDir, dir);
                                    }
                                    else
                                    {
                                        if (System.IO.File.Exists(callTreeDir))
                                        {
                                            string dir = System.IO.Path.Combine(client.Key, Utils.RunDir, System.IO.Path.GetFileName(callTreeDir));
                                            // find remote client
                                            if (System.IO.File.Exists(dir))
                                                System.IO.File.Delete(dir);
                                            System.IO.File.Move(callTreeDir, dir);
                                        }
                                    }

                                    // mark him busy
                                    lock (ClientStates)
                                    {
                                        ClientStates[client.Key] = Utils.CurrentState.BUSY;
                                    }

                                    // remove the task
                                    BplTasks.RemoveAt(0);

                                    // send the task to client
                                    clients[socketMap[client.Key]].Send(Utils.EncodeStr(Utils.StartWithCallTreeMsg + System.IO.Path.GetFileName(callTreeDir)));
                                }
                                else
                                {
                                    // remove the task
                                    BplTasks.RemoveAt(0);
                                }
                            }


                        }
                }
                lock (ClientStates)
                {
                    if (!ClientStates.Any(client => client.Value == Utils.CurrentState.BUSY))
                        break;
                }
            } 
            foreach (var c in clients)
            {
                c.Send(Utils.EncodeStr(Utils.DoneMsg));
            }
        }

        public static bool areClientsBusy()
        {
            lock (ClientStates)
            {
                if (ClientStates.Any(client => client.Value == Utils.CurrentState.BUSY))
                    return true;
            }
            return false;
        }

        public static void SendDoneMsg()
        { 
            foreach (var c in clients)
            {
                c.Send(Utils.EncodeStr(Utils.DoneMsg));
            }
        }

        public static void DeliverOneTask()
        {
            Dictionary<string, Utils.CurrentState> localClientStates;
            lock (BplTasks)
            {
                lock (ClientStates)
                {
                    localClientStates = new Dictionary<string, Utils.CurrentState>(ClientStates);
                }

                // clean the task list
                foreach (var client in localClientStates)
                    if (client.Value == Utils.CurrentState.AVAIL)
                    {
                        int removed = BplTasks.RemoveAll(task => task.author.Equals(client.Key));
                        //LogWithAddress.WriteLine(string.Format("Remove {0} from BplTask", removed));
                    }

                foreach (var client in localClientStates)
                    if (client.Value == Utils.CurrentState.AVAIL &&
                        BplTasks.Count > 0)
                    {
                        if (!BplTasks[0].author.Equals(client.Key))
                        {
                            BplTask topTask = BplTasks[0];
                            string callTreeDir = System.IO.Path.Combine(topTask.author, Utils.RunDir, topTask.callTreeDir);
                            // let him do the first task
                            if (System.IO.File.Exists(callTreeDir))
                            {
                                LogWithAddress.WriteLine(string.Format("Transfer {0} to {1}", topTask.ToString(), client.Key));

                                // pick the task from a client
                                // if localClient is the one

                                if (client.Key.Equals(config.root))
                                {
                                    string dir = System.IO.Path.Combine(config.root, Utils.RunDir, System.IO.Path.GetFileName(callTreeDir));
                                    if (System.IO.File.Exists(dir))
                                        System.IO.File.Delete(dir);
                                    try
                                    {
                                        System.IO.File.Move(callTreeDir, dir);
                                    }
                                    catch (Exception)
                                    {
                                        continue;
                                    }
                                }
                                else
                                {
                                    if (System.IO.File.Exists(callTreeDir))
                                    {
                                        string dir = System.IO.Path.Combine(client.Key, Utils.RunDir, System.IO.Path.GetFileName(callTreeDir));
                                        // find remote client
                                        if (System.IO.File.Exists(dir))
                                            System.IO.File.Delete(dir);
                                        try
                                        {
                                            System.IO.File.Move(callTreeDir, dir);
                                        }
                                        catch (Exception)
                                        {
                                            continue;
                                        }
                                    }
                                }

                                // mark him busy
                                lock (ClientStates)
                                {
                                    ClientStates[client.Key] = Utils.CurrentState.BUSY;
                                }

                                // send the task to client
                                clients[socketMap[client.Key]].Send(Utils.EncodeStr(Utils.StartWithCallTreeMsg + System.IO.Path.GetFileName(callTreeDir)));
                            }

                            // remove the task
                            //LogWithAddress.WriteLine(string.Format("Remove {0}", BplTasks[0].ToString()));
                            BplTasks.RemoveAt(0);


                            if (BplTasks.Count == 0)
                                break;
                        }
                        else
                        {
                            Debug.Assert(false);
                        }
                    }
            }
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.WriteLine("Got Ctrl-C");
            LogWithAddress.Close();
            lock (Utils.SpawnedProcesses)
            {
                //foreach (var p in Utils.SpawnedProcesses) 
                //{
                //    p.Kill();
                //}
                Utils.SpawnedProcesses.Clear();
            }
            System.Diagnostics.Process.GetCurrentProcess().Kill();
        }
    }
}
