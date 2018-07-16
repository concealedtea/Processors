using System;
using System.Linq;
using System.Data;
using System.Diagnostics;
using System.Net;
using Npgsql;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;
using UserMetricsProcessor.Helpers;

namespace UserMetricsProcessor
{
    class Processor
    {
        public static string Key1 = "REMOVED";
        public static string client_secret = Program.SetupParams["REMOVED"];
        public static string flatiron_affiliate_id = "REMOVED";
        public static string flatiron_api_key = "REMOVED";
        public static string propeller_api_username = "REMOVED";
        public static string propeller_api_password = "REMOVED";
        public static string content_ad_api_auth = "REMOVED";
        private static string diablo_api_key = "REMOVED";
        private static string _aws_access_key = "REMOVED";
        private static string _aws_secret_access_key = "REMOVED";
        private static string _connection_string = "REMOVED";
        private static string _archive_connection_string = "REMOVED";
        private static bool _debug = false;

        public static void UploadCampaignMetrics(DateTime StartDate, DateTime EndDate)
        {
            _debug = Environment.MachineName == "REMOVED" ? false : true;
            int et_offset = -5;
            using (SqlConnection conn = new SqlConnection(_connection_string))
            {
                conn.Open();
                SqlCommand cmd = new SqlCommand("SELECT DATEDIFF(HOUR,GETUTCDATE(), Reports.dbo.get_et()) AS eastern_time;", conn);
                cmd.CommandTimeout = 0;
                cmd.ExecuteNonQuery();
                using (var reader = cmd.ExecuteReader())
                {
                    if (reader != null && reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            et_offset = Convert.ToInt32(reader["eastern_time"].ToString());
                        }
                    }
                    reader.Close();
                }
                conn.Close();
            }
            string sql_file = _debug ? @"../../SQL/CampaignMetrics.sql" : @"C:/Processors/UserMetricsProcessor/SQL/CampaignMetrics.sql";
            string queryString = string.Format(File.ReadAllText(sql_file), StartDate.ToString("yyyy-MM-dd"), EndDate.ToString("yyyy-MM-dd"), et_offset);
            DataTable table = new DataTable();
            using (NpgsqlConnection conn = new NpgsqlConnection(_archive_connection_string))
            {
                NpgsqlCommand command = new NpgsqlCommand(queryString, conn);
                command.CommandTimeout = 0;
                conn.Open();
                NpgsqlDataAdapter da_Schedule = new NpgsqlDataAdapter(command);
                da_Schedule.Fill(table);
                conn.Close();
            }
            DataHelper.UploadDataTable(table, Program.SetupParams["push_sqlId"], Program.SetupParams["push_sqlPassword"], Program.SetupParams["PushDatabase"], "Upload_CampaignMetrics");
        }
    }
}
