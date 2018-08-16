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
        string clientIP;

        public ServerListener(Socket sk, string clientIP)
        {
            connection = sk;
            this.clientIP = clientIP;
        } 

        public void Listen()
        {
            var sep = new char[1];
            sep[0] = ':';

            if (connection != null)
            {
                string msg = "";
                while (!msg.Equals(Utils.CompletionMsg))
                {
                    byte[] data = new byte[Utils.MsgSize];
                    int receivedDataLength = connection.Receive(data); //Wait for the data from client
                    msg = Encoding.ASCII.GetString(data, 0, receivedDataLength); //Decode the data received
                    if (msg.Equals(Utils.CompletionMsg))
                    {
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientIP] = Utils.CurrentState.AVAIL;
                        }
                        // client completed his job
                        // tell client that he can quit
                        //connection.Send(Utils.EncodeStr(Utils.CompletionMsg));
                        //Finish();
                        //break;
                    }
                    else if (msg.Equals(Utils.DoneMsg))
                    {
                        lock (SplitParServer.ClientStates)
                        {
                            SplitParServer.ClientStates[clientIP] = Utils.CurrentState.AVAIL;
                        }
                        // clients & server completed his job
                        Finish();
                        break;
                    }
                    else
                    {
                        // new task is available
                        var split = msg.Split(sep);
                        lock (LogWithAddress.debugOut)
                        { 
                            if (split.Length > 1)
                            {
                                var fileName = split[1];
                                if (fileName.Contains(Utils.CallTreeSuffix))
                                {
                                    fileName = fileName.Substring(0, fileName.IndexOf(Utils.CallTreeSuffix)) + Utils.CallTreeSuffix;
                                    BplTask newTask = new BplTask(clientIP, fileName, int.Parse(split[0]));
                                    LogWithAddress.WriteLine(string.Format(newTask.ToString())); //Write the data on the screen
                                                                                                 // add a new task
                                    lock (SplitParServer.BplTasks)
                                    {
                                        SplitParServer.BplTasks.Add(newTask);
                                    }
                                }
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
                connection.Send(Utils.EncodeStr(Utils.CompletionMsg));
                connection.Close();
            }
        } 
    }
}
