using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using UserMetricsProcessor.Helpers;


namespace UserMetricsProcessor
{
    class Program
    {
        public static Dictionary<String, String> SetupParams = new Dictionary<String, String>();

        static void Main(string[] args)
        {
            foreach (String key in ConfigurationManager.AppSettings.AllKeys.ToArray())
            {
                SetupParams[key] = ConfigurationManager.AppSettings[key].Expand();
            }
            SetupParams["reportDate"] = DateTime.Now.ToString("yyyy-MM-dd");


            DateTime startDate = DateTime.Now.AddDays(-2);
            DateTime endDate = DateTime.Now.AddDays(-1);

            //DateTime startDate1 = Convert.ToDateTime("2018-04-01");
            //DateTime endDate1 = DateTime.Now.AddDays(0);

            //foreach (string dates in new string[] { "2018-04-01|2018-04-30", "2018-05-01|2018-05-31", "2018-06-01|2018-06-30", "2018-07-01|2018-07-31" })
            //{
            //    Console.WriteLine(dates);
            //    Processor.UploadCampaignMetrics(Convert.ToDateTime(dates.Split('|')[0]), Convert.ToDateTime(dates.Split('|')[1]));
            //}

            Processor.UploadCampaignMetrics(startDate, endDate);
        }
    }
}
