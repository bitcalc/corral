using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using CommonLib;
using System.Threading.Tasks;
using System.Threading;

namespace SplitParClient
{
    enum CurrentState { AVAIL, BUSY };
    class SplitParClient
    {
        static System.Collections.Concurrent.ConcurrentQueue<WorkItem> work = new System.Collections.Concurrent.ConcurrentQueue<WorkItem>();
        static CurrentState state = CurrentState.AVAIL;
        static Socket serverConnection;
        static Socket corralConnection;
        static HashSet<string> jobList = new HashSet<string>();
        static SplitParConfig config;
        static bool testWithoutServer = false;
        static bool testWithoutCorral = false; 

        static void Main(string[] args)
        {
            Console.CancelKeyPress += Console_CancelKeyPress;

            #region for testing purposes
            for (int i = 0; i < args.Length; ++i)
                if (args[i].Equals(Utils.NoServer))
                    testWithoutServer = true;
                else if (args[i].Equals(Utils.NoCorral))
                    testWithoutCorral = true;
            #endregion

            config = Utils.LoadConfig(args[0]);
            LogWithAddress.init(System.IO.Path.Combine(config.root, Utils.RunDir));
            ClientController(args);
            LogWithAddress.Close();
        }

        static void ClientController(params string[] args)
        {
            if (false)
            {
                #region do not use it
                //Debug.Assert(args.Length > 0);
                //ConnectServer(args[0]);
                #endregion
            }
            else
            {
                if (!testWithoutServer)
                    ConnectServer();
            }

            if (!testWithoutCorral)
            {
                if (!Utils.SocketConnected(corralConnection))
                { 
                    StartCorral();
                }
                Thread.Sleep(Utils.SleepTime);
            }

            ConnectCorral();

            string msg = Utils.StartMsg;
            int cnt = 0;
            while (msg != Utils.DoneMsg)
            {
                cnt = cnt + 1;
                // wait for a message
                if (!testWithoutServer)
                {
                    byte[] data = new byte[Utils.MsgSize];
                    int receivedDataLength = serverConnection.Receive(data); // wait for the data from server
                    msg = Encoding.ASCII.GetString(data, 0, receivedDataLength); // decode the data received
                    LogWithAddress.WriteLine(string.Format("{0}", msg)); // log data 
                }
                if (msg.Equals(Utils.DoneMsg))
                {
                    // receive a shutdown signal                       
                    // tell server that he doesnt need to wait
                    if (!testWithoutServer) 
                        serverConnection.Send(Utils.EncodeStr(Utils.DoneMsg));
                    corralConnection.Send(Utils.EncodeStr(Utils.DoneMsg));
                }
                else if (msg.Contains(Utils.StartMsg))
                {
                    // receive a working signal
                    if (!msg.Equals(Utils.StartMsg))
                    {
                        // start with call tree
                        var sep = new char[1];
                        sep[0] = ':';
                        var split = msg.Split(sep);

                        if (split.Length > 1)
                        {
                            Debug.Assert(config.Utils.Count == 1);
                            config.Utils[0].arguments = config.Utils[0].arguments;
                            //config.Utils[0].arguments = config.Utils[0].arguments + " /prevSIState:" + split[1];
                        }
                    }                   

                    // give corral a task
                    SendTask(msg);
                    MonitoringCorral();

                    // send completion msg to server 
                    if (!testWithoutServer)
                        serverConnection.Send(Utils.EncodeStr(Utils.CompletionMsg));
                    else
                    {
                        // testing purpose
                        switch (cnt)
                        {
                            case 1:
                                msg = Utils.StartMsg;
                                break;
                            case 2:
                                msg = Utils.StartWithCallTreeMsg + "1split.txt";
                                break;
                            case 3:
                                msg = Utils.StartWithCallTreeMsg + "2split.txt";
                                break;
                            case 4:
                                msg = Utils.StartWithCallTreeMsg + "6split.txt";
                                break;
                            case 5:
                                msg = Utils.StartWithCallTreeMsg + "7split.txt";
                                break;
                            case 6:
                                msg = Utils.StartWithCallTreeMsg + "90split.txt";
                                break;
                            case 7:
                                msg = Utils.StartWithCallTreeMsg + "98split.txt";
                                break;
                            default:
                                msg = Utils.DoneMsg;
                                corralConnection.Send(Utils.EncodeStr(Utils.DoneMsg));
                                break;
                        }
                    }
                }
            }
            if (corralConnection != null && corralConnection.Connected)
            {
                corralConnection.Close();
            }
        }

        static void ConnectServer()
        {
            LogWithAddress.WriteLine(string.Format("Connecting server"));
            IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
            IPAddress ipAddress = ipHostInfo.AddressList[0];
            IPEndPoint localEndPoint = new IPEndPoint(ipAddress, Utils.ServerPort);


            serverConnection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            try
            {
                serverConnection.Bind(localEndPoint);
                serverConnection.Listen(10);

                LogWithAddress.WriteLine(string.Format("Waiting for a connection..."));
                serverConnection = serverConnection.Accept();
                LogWithAddress.WriteLine(string.Format("Connected"));
                serverConnection.Send(Utils.EncodeStr("Hello " + serverConnection.RemoteEndPoint.ToString() + " from " + Dns.GetHostName().ToString()));

                // wait for the reply message
                byte[] data = new byte[Utils.MsgSize];
                int receivedDataLength = serverConnection.Receive(data); //Wait for the data
                string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received

                LogWithAddress.WriteLine(string.Format("{0}", stringData)); //Write the data on the screen
            }
            catch
            {
                LogWithAddress.WriteLine(string.Format("Error"));
            }
        }

        static void ConnectServer(string ipHost)
        {
            try
            {

                IPHostEntry ipHostInfo = Dns.Resolve(ipHost);
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, Utils.ServerPort);


                serverConnection = new Socket(AddressFamily.InterNetwork,
                    SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    serverConnection.Connect(remoteEP);

                    LogWithAddress.WriteLine(string.Format("Socket connected {0}", serverConnection.RemoteEndPoint.ToString()));

                    byte[] data = new byte[Utils.MsgSize];
                    int receivedDataLength = serverConnection.Receive(data); //Wait for the data
                    string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                    LogWithAddress.WriteLine(string.Format("{0}", stringData)); //Write the data on the screen

                    // reply the server
                    serverConnection.Send(Utils.EncodeStr("Hi " + serverConnection.RemoteEndPoint.ToString()));
                }
                catch (ArgumentNullException ane)
                {
                    LogWithAddress.WriteLine(string.Format("ArgumentNullException : {0}", ane.ToString()));
                }
                catch (SocketException se)
                {
                    LogWithAddress.WriteLine(string.Format("SocketException : {0}", se.ToString()));
                }
                catch (Exception e)
                {
                    LogWithAddress.WriteLine(string.Format("Unexpected exception : {0}", e.ToString()));
                }

            }
            catch
            {
                LogWithAddress.WriteLine(string.Format("Cannot connect to Server"));
            }

        }

        static void MonitoringCorral()
        {
            var sep = new char[1];
            sep[0] = ':';

            string msg = "";
            while (!msg.Equals(Utils.CompletionMsg))
            {
                if (!Utils.SocketConnected(corralConnection))
                { 
                    break;
                }

                byte[] data = new byte[Utils.MsgSize];
                int receivedDataLength = corralConnection.Receive(data); //Wait for the data from corral
                msg = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                if (msg.Equals(Utils.CompletionMsg))
                {
                    // we dont close because it will be used for future msg
                    // corralConnection.Close();
                    break;
                }
                var split = msg.Split(sep);

                if (split.Length > 1)
                {
                    if (split[0].Equals(Utils.DoingMsg))
                    {
                        LogWithAddress.WriteLine(string.Format(Utils.DoingMsg + ":" + split[1])); //Write the data on the screen
                        //Debug.Assert(jobList.Contains(split[1])); // some package can be lost
                    }
                    else
                    {
                        // inform server when new tasks are available
                        if (!testWithoutServer)
                            serverConnection.Send(Utils.EncodeStr(msg));
                        // log data
                        LogWithAddress.WriteLine(string.Format(Utils.Indent(int.Parse(split[0])) + ">>> " + split[1]));
                        jobList.Add(split[1]);
                    }
                }
            }
            LogWithAddress.WriteLine(string.Format("{0}", msg));
        }

        void SpawnCorral()
        {
            var loc = System.IO.Path.Combine(config.root, Utils.CorralDir, Utils.CorralExe);

            var tmp = System.IO.Path.Combine(config.root, Utils.RunDir);
            System.IO.Directory.CreateDirectory(tmp);

            var flags = "";// config.Utils.ar.Aggregate("", ((s1, s2) => s1 + " " + s2)); 

            var outp = Utils.run(tmp, loc, flags);

            //var resultsfile = System.IO.Path.Combine(tmp, Utils.ResultFile);
            //var ex = System.IO.File.Exists(resultsfile); 
        }

        static void StartCorral()
        {
            var threads = new List<Thread>();
            var workers = new List<WorkItem>();

            var starttime = DateTime.Now;
            Console.WriteLine("Spawning Corral");

            // spawn client on own machine            
            Debug.Assert(config.Utils.Count == 1);
            foreach (var util in config.Utils)
            {             
                // pick the first file in BoogieFiles to handle
                Debug.Assert(config.BoogieFiles.Count > 0);
                var file = System.IO.Path.Combine(config.root, Utils.DataDir, config.BoogieFiles.ElementAt(0).value);

                var args = file + " " + util.arguments;
                var w0 = new WorkItem("local", config.root, args);
                workers.Add(w0);
                threads.Add(new Thread(new ThreadStart(w0.Run)));
            }
            // start threads
            threads.ForEach(t => t.Start());
        }

        static void PongServer()
        {

            switch (state)
            {
                case CurrentState.AVAIL:
                    serverConnection.Send(Utils.EncodeStr(Utils.ReadyMsg));
                    break;
                case CurrentState.BUSY:
                    serverConnection.Send(Utils.EncodeStr(Utils.NotReadyMsg));
                    break;
                default:
                    Debug.Assert(false);
                    break;
            }
        }

        static void InformServerWhenCompleted()
        {
            state = CurrentState.AVAIL;
            serverConnection.Send(Utils.EncodeStr(Utils.CompletionMsg));
        }

        static bool CheckIfWorking(string taskName)
        {
            // Are we already running?
            //System.Diagnostics.Process.GetCurrentProcess().ProcessName
            var procs =
                System.Diagnostics.Process.GetProcessesByName(taskName);
            if (procs.Count() > 1)
            {
                //return;
            }
            return false;
        }

        static void SendTask(string msg)
        {
            LogWithAddress.WriteLine(string.Format("Send a task."));
            if (Utils.SocketConnected(corralConnection))
            {
                // give corral a task
                corralConnection.Send(Utils.EncodeStr(msg));

                // corral will reply "working"
                byte[] data = new byte[Utils.MsgSize];
                int receivedDataLength = corralConnection.Receive(data); //Wait for the data
                string stringData = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                LogWithAddress.WriteLine(string.Format("{0}", stringData)); //Write the data on the screen
            }
        }

        static void ConnectCorral()
        {
            LogWithAddress.WriteLine(string.Format("Connecting Corral"));

            try
            {
                IPHostEntry ipHostInfo = Dns.Resolve(Dns.GetHostName());
                IPAddress ipAddress = ipHostInfo.AddressList[0];
                IPEndPoint remoteEP = new IPEndPoint(ipAddress, Utils.CorralPort);

                while (true)
                {
                    corralConnection = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                    try
                    {
                        corralConnection.Connect(remoteEP);

                        LogWithAddress.WriteLine(string.Format("Socket connected {0}", corralConnection.RemoteEndPoint.ToString())); 
                        break;
                    }
                    catch (SocketException se)
                    {
                        LogWithAddress.WriteLine(string.Format("Corral is not connected yet."));
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
                LogWithAddress.WriteLine(string.Format("Cannot connect to Corral"));
            }
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.WriteLine("Got Ctrl-C");
            LogWithAddress.Close();
            lock (Utils.SpawnedProcesses)
            {
                foreach (var p in Utils.SpawnedProcesses)
                    p.Kill();
                Utils.SpawnedProcesses.Clear();
            }
            System.Diagnostics.Process.GetCurrentProcess().Kill();
        }
    }
}
