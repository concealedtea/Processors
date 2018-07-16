using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Configuration;
using System.Security.Principal;
using System.Security.AccessControl;
using System.Threading;

namespace UserMetricsProcessor.Helpers
{
    class DataHelper
    {
        //==========================================================================//
        //  LOGGING RELATED FUNCTIONS                                               //
        //==========================================================================//

        // Record operations to log file
        public static void WriteLog(String logMsg, String reportName = "Spigot")
        {
            Console.WriteLine(logMsg);
            String LogFolderPath = ConfigurationManager.AppSettings["logFolder"] + DateTime.Now.ToString("yyyy-MM-dd");
            try
            {
                //Check if the log folder exists, and create it if it does not
                if (!Directory.Exists(LogFolderPath))
                {
                    Directory.CreateDirectory(LogFolderPath);
                    DirectorySecurity securityRules = Directory.GetAccessControl(LogFolderPath);
                    SecurityIdentifier everyone = new SecurityIdentifier(WellKnownSidType.WorldSid, null);
                    securityRules.AddAccessRule(new FileSystemAccessRule(everyone, FileSystemRights.Modify | FileSystemRights.Synchronize, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
                    Directory.SetAccessControl(LogFolderPath, securityRules);
                }
            }
            catch
            {
                LogFolderPath = AppDomain.CurrentDomain.BaseDirectory;
            }
            using (StreamWriter sw = new StreamWriter(LogFolderPath + "\\" + reportName + ".log", true))
            {
                try
                {
                    sw.Write(Thread.CurrentThread.ManagedThreadId);
                    sw.Write("\t");
                    sw.Write(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss tt"));
                    sw.Write("\t");
                    sw.WriteLine(logMsg);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        // Write to log file
        public static void WriteErrorLog(string Message)
        {
            Directory.GetAccessControl("C:\\Processors");
            String LogFolderPath = ConfigurationManager.AppSettings["logFolder"];
            String CurrentLogFolder = LogFolderPath + DateTime.Today.ToString("yyyy-MM-dd") + "\\";
            if (LogFolderPath == null)
            {
                try
                {
                    //Check if the log folder exists, and create it if it does not
                    if (!Directory.Exists(LogFolderPath))
                    {
                        Directory.CreateDirectory(LogFolderPath);
                    }
                    //Check if the log folder exists, and create it if it does not
                    if (!Directory.Exists(CurrentLogFolder))
                    {
                        Directory.CreateDirectory(CurrentLogFolder);
                    }
                }
                catch
                {
                    LogFolderPath = AppDomain.CurrentDomain.BaseDirectory;
                }

                // Attempt to get a list of security permissions from the folder. 
                // This will raise an exception if the path is read only or does not have access to view the permissions. 
            }
            //Check if the log folder exists, and create it if it does not
            if (!Directory.Exists(LogFolderPath))
            {
                Directory.CreateDirectory(LogFolderPath);
            }
            //Check if the log folder exists, and create it if it does not
            if (!Directory.Exists(CurrentLogFolder))
            {
                Directory.CreateDirectory(CurrentLogFolder);
            }

            Directory.GetAccessControl(LogFolderPath);
            Directory.GetAccessControl(CurrentLogFolder);

            StreamWriter sw = null;
            foreach (String LogFile in new String[] { LogFolderPath, CurrentLogFolder })
            {
                try
                {
                    // Add log line to E-Mail body
                    // if (LogFile == CurrentLogFolder) { Reporting.MailBody += DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss tt") + "\t" + Message + "\n"; }
                    // Write log to Log File
                    sw = new StreamWriter(LogFile + ConfigurationManager.AppSettings["logFile"].Expand(), true);
                    sw.WriteLine(String.Join("\t", new String[] { Thread.CurrentThread.ManagedThreadId.ToString(), DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss tt"), Message }));
                    if (LogFile == LogFolderPath) { Console.WriteLine(String.Join("\t", new String[] { DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss tt"), Message })); }
                    sw.Flush();
                    sw.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Logging error: " + ex.Message);
                }
            }
        }

        public static void UploadDataTable(DataTable dt, string user_id, string user_pw, string db, string table, string datasource = "push.ctwzprc2znex.us-east-1.rds.amazonaws.com")
        {
            try
            {
                using (SqlBulkCopy bulkCopy = new SqlBulkCopy("Password=" + user_pw + ";Persist Security Info=True;User ID=" + user_id + ";Initial Catalog=" + db + ";Data Source=" + datasource + ";", SqlBulkCopyOptions.FireTriggers))
                {
                    bulkCopy.BulkCopyTimeout = 0;
                    bulkCopy.DestinationTableName = db + ".dbo." + table;
                    bulkCopy.WriteToServer(dt);
                    //WriteErrorLog("Upload Data Table - " + table);
                }
            }
            catch (Exception e)
            {
                WriteErrorLog("Exception " + e.Message);
            }
        }

        public static void ExecuteQuery(string QueryText, string user_id, string user_pw, string db)
        {
            try
            {
                string ConnectionString = String.Format("Server=" + ConfigurationManager.AppSettings["ServerName"] + ";Database=" + db + ";User Id=" + user_id + ";Password=" + user_pw + ";");
                // Execute the SQL Function for LTVs
                using (SqlConnection conn = new SqlConnection(ConnectionString))
                {
                    conn.Open();
                    SqlCommand cmd = new SqlCommand(QueryText, conn);
                    cmd.CommandTimeout = 0;
                    cmd.ExecuteNonQuery();
                    conn.Close();
                }
            }
            catch (Exception e)
            {
                WriteErrorLog("Exception " + e.Message);
            }
        }
    }
}
