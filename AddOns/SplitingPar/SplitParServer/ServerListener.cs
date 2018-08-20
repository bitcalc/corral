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
        Socket connection = null;
        string clientAddress;

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
                        byte[] data = new byte[Utils.MsgSize];
                        int receivedDataLength = connection.Receive(data); //Wait for the data from client
                        msg = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                    }
                    catch (Exception)
                    {
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientAddress] = Utils.CurrentState.AVAIL;
                        }
                        LogWithAddress.WriteLine(string.Format("{0} close because of an unknown reason: {1}", clientAddress, msg));
                        Finish();
                        break;
                    }
                    
                    if (msg.Equals(Utils.CompletionMsg))
                    {
                        // client completed his job
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientAddress] = Utils.CurrentState.AVAIL;
                        }

                        LogWithAddress.WriteLine(string.Format("Client {0} completed", clientAddress));
                        if (SplitParServer.areClientsBusy())
                        {
                            SplitParServer.DeliverOneTask();
                        }
                        else
                            SplitParServer.SendDoneMsg();
                    }
                    else if (msg.Equals(Utils.DoneMsg))
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
