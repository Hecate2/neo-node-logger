using Neo.Ledger;
using Neo.Network.P2P;
using Neo.Network.P2P.Payloads;
using Neo.Persistence;

namespace Neo.Plugins
{
    public class NeoNodeLogger : Plugin
    {
        static DateTime s_previousDateTime = DateTime.UtcNow;
        static string logFilePath = "./Plugins/NeoNodeLogger/" + s_previousDateTime.ToString("yyyy-MM-dd") + ".log.txt";
        static StreamWriter logFileWriter = new StreamWriter(logFilePath, append: true);
        readonly Dictionary<MessageCommand, int> _messageSizeCount = new();
        protected void WriteLog(string message, DateTime? assignedTime = null)
        {
            DateTime now;
            if (assignedTime == null)
                now = DateTime.UtcNow;
            else
                now = (DateTime)assignedTime;
            if (now.Date != s_previousDateTime.Date)
            {
                s_previousDateTime = now;
                logFilePath = "./Plugins/NeoNodeLogger/" + now.ToString("yyyy-MM-dd") + ".log.txt";
                logFileWriter.Flush();
                logFileWriter.Close();
                logFileWriter = new StreamWriter(logFilePath, append: true);
            }
            logFileWriter.WriteLine($"{now:yyyy-MM-dd HH:mm:ss.fffffff} {message}");
            //Console.WriteLine(message);
        }

        protected void OnTransactionAdded(object? sender, Transaction tx) => WriteLog($"Tx added: {tx.Hash}; Size: {tx.Size}");

        protected void OnTransactionRemoved(object? sender, TransactionRemovedEventArgs removedEventArgs)
        {
            DateTime now = DateTime.UtcNow;
            foreach (Transaction tx in removedEventArgs.Transactions)
                WriteLog($"Tx removed: {tx}; Reason: {removedEventArgs.Reason}", now);
        }

        protected bool OnRemoteMessage(NeoSystem system, Message message)
        {
            if (!_messageSizeCount.TryGetValue(message.Command, out int count))
            {
                if (message.Payload != null)
                    _messageSizeCount[message.Command] = message.Payload.Size;
                else
                    _messageSizeCount[message.Command] = 1;
                return true;
            }
            if (message.Payload != null)
                _messageSizeCount[message.Command] = count + message.Payload.Size;
            //WriteLog($"{message.Command} {message.Payload?.Size}");
            else
                _messageSizeCount[message.Command] = count + 1;
            //WriteLog($"{message.Command}");
            return true;
        }

        private void DumpMessage(NeoSystem system, Block block, DataCache snapshot, IReadOnlyList<Blockchain.ApplicationExecuted> applicationExecutedList)
        {
            DateTime now = DateTime.UtcNow;
            WriteLog($"Committing block {block.Index} with {block.Transactions.Length} txs of total size {block.Transactions.Sum(t => t.Size)}", now);
            foreach ((MessageCommand cmd, int size) in _messageSizeCount)
            {
                if (size > 0)
                    WriteLog($"Message {cmd} of total size {size}", now);
                _messageSizeCount[cmd] = 0;
            }
            logFileWriter.Flush();
        }

        protected override void OnSystemLoaded(NeoSystem system)
        {
            Console.WriteLine($"{nameof(NeoNodeLogger)} activated");
            RemoteNode.MessageReceived += OnRemoteMessage;
            system.MemPool.TransactionAdded += OnTransactionAdded;
            system.MemPool.TransactionRemoved += OnTransactionRemoved;
            Blockchain.Committing += DumpMessage;
        }

        public override void Dispose()
        {
            logFileWriter.Flush();
        }
    }
}
