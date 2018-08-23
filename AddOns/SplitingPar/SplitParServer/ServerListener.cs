using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using CommonLib;

namespace SplitParServer
{
    public class ServerListener
    {
        public Socket connection = null;
        public string clientAddress;
        public string currentResult = "OK";

        public ServerListener(Socket sk, string clientAddress)
        {
            connection = sk;
            this.clientAddress = clientAddress;
        }

        public void Listen()
        {
            var sep = new char[1];
            sep[0] = ':';

            if (connection != null)
            {
                string msg = "";
                while (!msg.Contains(Utils.DoneMsg))
                {                     
                    try
                    {
                        //Wait for the data from client
                        byte[] data = new byte[Utils.MsgSize];
                        int receivedDataLength = connection.Receive(data);
                        msg = Encoding.ASCII.GetString(data, 0, receivedDataLength);  
                    }
                    catch (Exception)
                    {
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientAddress] = Utils.CurrentState.AVAIL;
                        }
                        LogWithAddress.WriteLine(string.Format("{0} close because a counterexample is found: {1}", clientAddress, msg));
                        Finish();
                        break;
                    }

                    if (msg.Contains(Utils.CompletionMsg))
                    {
                        // client completed his job
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientAddress] = Utils.CurrentState.AVAIL;
                        }

                        // parse client message: Complete:OK|NOK|RB
                        var split = msg.Split(sep);
                        if (split.Length > 1)
                        {
                            if (split[1].Equals("NOK"))
                            {
                                // kill all clients if they are running
                                SplitParServer.ForceClose();
                                currentResult = "NOK";
                                break;
                            }
                            else if (split[1].Equals("RB"))
                                currentResult = "RB";
                        }
                        LogWithAddress.WriteLine(string.Format("Client {0} completed", clientAddress));
                        if (SplitParServer.areClientsBusy())
                        {
                            SplitParServer.DeliverOneTask();
                        }
                        else
                        {
                            SplitParServer.SendDoneMsg();
                        }
                    }
                    else if (msg.Contains(Utils.DoneMsg))
                    {
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientAddress] = Utils.CurrentState.AVAIL;
                        }
                        LogWithAddress.WriteLine(string.Format("{0}: {1}", clientAddress, msg));
                        // clients & server completed their job
                        Finish();
                        break;
                    }
                    else if (msg.Contains(Utils.DoingMsg))
                    {
                        // task to remove
                        var split = msg.Split(sep);
                        //LogWithAddress.WriteLine(msg);
                        if (split.Length > 1)
                        {
                            var fileName = split[1];
                            if (fileName.Contains(Utils.CallTreeSuffix))
                            {
                                fileName = fileName.Substring(0, fileName.IndexOf(Utils.CallTreeSuffix)) + Utils.CallTreeSuffix;
                                lock (SplitParServer.BplTasks)
                                {
                                    for (int i = 0; i < SplitParServer.BplTasks.Count; ++i)
                                        if (SplitParServer.BplTasks[i].author.Equals(clientAddress) &&
                                            SplitParServer.BplTasks[i].callTreeDir.Equals(fileName))
                                        {
                                            SplitParServer.BplTasks.RemoveAt(i);
                                            break;
                                        }
                                }
                            }
                        }
                    }
                    else
                    {
                        // new task is available
                        var split = msg.Split(sep);
                        if (split.Length > 1)
                        {
                            var fileName = split[1];
                            if (fileName.Contains(Utils.CallTreeSuffix))
                            {
                                fileName = fileName.Substring(0, fileName.IndexOf(Utils.CallTreeSuffix)) + Utils.CallTreeSuffix;
                                BplTask newTask = new BplTask(clientAddress, fileName, int.Parse(split[0]));
                                //LogWithAddress.WriteLine(string.Format(newTask.ToString()));

                                // add a new task
                                lock (SplitParServer.BplTasks)
                                {
                                    SplitParServer.BplTasks.Add(newTask);
                                    //LogWithAddress.WriteLine(string.Format("Add new task: {0}", newTask.ToString()));
                                }
                                if (SplitParServer.areClientsAvail())
                                    SplitParServer.DeliverOneTask();
                            }
                        }
                    }
                }
            }
        }

        public void Finish()
        {
            if (connection != null)
            {
                //connection.Shutdown(SocketShutdown.Both);
                connection.Close();
            }
        }
    }
}
