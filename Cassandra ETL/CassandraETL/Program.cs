using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FluentCassandra;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using CassandraETL.Model;

namespace CassandraETL
{
    class Program
    {
        static void Main(string[] args)
        {
            Program p = new Program();

            FluentCassandra.Connections.Server svr = new FluentCassandra.Connections.Server("54.186.36.251");
           
            FluentCassandra.CassandraSession session = new CassandraSession("CloudDataMgt",svr);
            var db = new CassandraContext(session);
            Console.WriteLine(db.Keyspace.KeyspaceName.ToString());
            Console.WriteLine("Time Started: " + DateTime.Now.ToString());
            p.getHighwayStations(db);
            //stations.CreateRecord()
            //dynamic thisWorker = workers.CreateRecord(key: "11224224");
            //dynamic workerDetail = thisWorker
            //Console.WriteLine(db.DescribeClusterName());


            DataTable detectors = GetDataTableFromCsv("c:\\incoming\\freeway_detectors.csv", true);


            string file_name = "c:\\incoming\\freeway_loopdata.csv";
            List<LoopData> loop = new List<LoopData>();
            int rowcount = 0;
            int batchCout = 0;
            int batchSize = 1000;
            using (StreamReader reader = new StreamReader(file_name))
            {
                string line = null;
                while ((line = reader.ReadLine()) != null)
                {
                    String[] thisRow = line.Split(',');
                    //If speed is not null and this isn't the column header row. 
                    if (thisRow[3] != "speed" && thisRow[3] != "")
                    {

                        LoopData thisReading = new LoopData();
                        thisReading.DetectorID = int.Parse(thisRow[0]);
                        thisReading.Speed = int.Parse(thisRow[3]);
                        thisReading.Volume = int.Parse(thisRow[2]);

                        DateTime thisDate = DateTime.Parse(thisRow[1]);
                        thisReading.StartHour = (byte)thisDate.Hour;
                        thisReading.StartTime = thisDate.TimeOfDay.ToString();
                        thisReading.StartMinute = (byte)thisDate.Minute;
                        thisReading.StartSecond = (byte)thisDate.Second;
                        thisReading.StartDate = thisDate.Year.ToString() + "-" + thisDate.Month.ToString() + "-" + thisDate.Day.ToString();
                        thisReading.DayOfWeek = getDayAbbrev(thisDate.DayOfWeek);
                        loop.Add(thisReading);

                        //Console.WriteLine(loop.Count.ToString());

                    }


                }
            }

            var LoopDetectors = from table1 in detectors.AsEnumerable()
                                join table2 in loop.AsEnumerable() on (int)table1["detectorid"] equals table2.DetectorID
                                select new
                                {
                                    StationID = (int)table1["stationid"],
                                    DetectorID = (int)table1["detectorid"],
                                    HighwayID = (int)table1["highwayid"],
                                    StartDate = table2.StartDate,
                                    StartTime = table2.StartTime,
                                    StartHour = table2.StartHour,
                                    StartMinute = table2.StartMinute,
                                    StartSecond= table2.StartSecond,
                                    DayOfWeek = table2.DayOfWeek,
                                    Volume = table2.Volume,
                                    Speed = table2.Speed
                                };


            foreach (var item in LoopDetectors)
            {
                string query = "INSERT INTO \"CloudDataMgt\".\"LoopData\" ( \"StationID\",\"HighwayID\",\"DetectorID\", \"StartDate\",\"StartTime\",\"StartHour\",\"DayOfWeek\",\"Volume\",\"Speed\",\"StartMinute\",\"StartSecond\") ";
                query += "VALUES( " + item.StationID.ToString() + ", " + item.HighwayID.ToString() + ", " + item.DetectorID.ToString() + ", '" + item.StartDate + "', '" + item.StartTime + "', " + item.StartHour + ", '" + item.DayOfWeek + "'," + item.Volume + "," + item.Speed + "," + item.StartMinute + "," + item.StartSecond + ")";

                db.ExecuteNonQuery(query);
            }




            Console.WriteLine("Time Finished: " + DateTime.Now.ToString());
          
            
            Console.Read();
        }


        private void dealWithStationLoops()
        {


            DataTable detectors = GetDataTableFromCsv("c:\\incoming\\freeway_detectors.csv", true);


            string file_name = "c:\\incoming\\freeway_loopdata.csv";
            List<LoopData> loop = new List<LoopData>();
            int rowcount = 0;
            int batchCout = 0;
            int batchSize = 1000;
            using (StreamReader reader = new StreamReader(file_name))
            {
                string line = null;
                while ((line = reader.ReadLine()) != null)
                {
                    String[] thisRow = line.Split(',');
                    //If speed is not null and this isn't the column header row. 
                    if (thisRow[3] != "speed" && thisRow[3] != "")
                    {

                        LoopData thisReading = new LoopData();
                        thisReading.DetectorID = int.Parse(thisRow[0]);
                        thisReading.Speed = int.Parse(thisRow[3]);
                        thisReading.Volume = int.Parse(thisRow[2]);

                        DateTime thisDate = DateTime.Parse(thisRow[1]);
                        thisReading.StartHour = (byte)thisDate.Hour;
                        thisReading.StartTime = thisDate.TimeOfDay.ToString();
                        thisReading.StartDate = thisDate.Date.ToShortDateString();
                        thisReading.DayOfWeek = getDayAbbrev(thisDate.DayOfWeek);
                        loop.Add(thisReading);

                        //Console.WriteLine(loop.Count.ToString());

                    }


                }
            }

            var LoopDetectors = from table1 in detectors.AsEnumerable()
                                join table2 in loop.AsEnumerable() on (int)table1["detectorid"] equals table2.DetectorID
                                select new
                                {
                                    StationID = (int)table1["stationid"],
                                    DetectorID = (int)table1["detectorid"],
                                    HighwayID = (int)table1["highwayid"],
                                    StartDate = table2.StartDate,
                                    StartTime = table2.StartTime,
                                    StartHour = table2.StartHour,
                                    DayOfWeek = table2.DayOfWeek,
                                    Volume = table2.Volume,
                                    Speed = table2.Speed
                                };


            foreach (var item in LoopDetectors)
            {


            }

        }
        private static string getDayAbbrev(DayOfWeek theDay)
        {
            if (theDay == DayOfWeek.Friday)
                return "Friday";
            else if (theDay == DayOfWeek.Monday)
                return "Monday";
            else if (theDay == DayOfWeek.Tuesday)
                return "Tuesday";
            else if (theDay == DayOfWeek.Wednesday)
                return "Wednesday";
            else if (theDay == DayOfWeek.Thursday)
                return "Thursday";
            else if (theDay == DayOfWeek.Saturday)
                return "Saturday";
            else if (theDay == DayOfWeek.Sunday)
                return "Sunday";
            else return null;
        }

        public void getHighwayStations(CassandraContext db) {

            DataTable Highway = GetDataTableFromCsv("c:\\incoming\\highways.csv", true);
            DataTable Stations = GetDataTableFromCsv("c:\\incoming\\freeway_stations.csv", true, "stationclass = 1");


            var HighwayStations = from table1 in Highway.AsEnumerable()
                                  join table2 in Stations.AsEnumerable() on (int)table1["highwayid"] equals (int)table2["highwayid"]
                                  select new
                                  {
                                      StationID = (int)table2["stationid"],
                                      HighwayID = (int)table2["highwayid"],
                                      DownstreamStationID = (int)table2["downstream"],
                                      LocationText = (string)table2["locationtext"],
                                      LengthMid = (double?)table2["length_mid"],
                                      ShortDirection = (string)table1["shortdirection"],
                                      HighwayName = (string)table1["highwayname"]
                                  };


            var stations = db.GetColumnFamily("Stations");

            foreach (var item in HighwayStations)
            {
                string query = "INSERT INTO \"CloudDataMgt\".\"Stations\" ( \"StationID\",\"HighwayID\",\"DownstreamStationID\", \"LocationText\",\"LengthMid\",\"ShortDirection\",\"HighwayName\") ";
                query += "VALUES( " + item.StationID.ToString() + ", " + item.HighwayID.ToString() + ", " + item.DownstreamStationID.ToString() + ", '" + item.LocationText + "', " + item.LengthMid.ToString() + ", '" + item.ShortDirection + "', '" + item.HighwayName + "');";


                db.ExecuteNonQuery(query);
            }
        
        }


        static DataTable GetDataTableFromCsv(string path, bool isFirstRowHeader, string filter)
        {
            string header = isFirstRowHeader ? "Yes" : "No";

            string pathOnly = Path.GetDirectoryName(path);
            string fileName = Path.GetFileName(path);

            string sql = @"SELECT * FROM [" + fileName + "] WHERE " + filter ;

            using (OleDbConnection connection = new OleDbConnection(
                      @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + pathOnly +
                      ";Extended Properties=\"Text;HDR=" + header + "\""))
            using (OleDbCommand command = new OleDbCommand(sql, connection))
            using (OleDbDataAdapter adapter = new OleDbDataAdapter(command))
            {
                DataTable dataTable = new DataTable();
                dataTable.Locale = CultureInfo.CurrentCulture;
                adapter.Fill(dataTable);
                return dataTable;
            }
        }
        static DataTable GetDataTableFromCsv(string path, bool isFirstRowHeader)
        {
            string header = isFirstRowHeader ? "Yes" : "No";

            string pathOnly = Path.GetDirectoryName(path);
            string fileName = Path.GetFileName(path);

            string sql = @"SELECT * FROM [" + fileName + "]";

            using (OleDbConnection connection = new OleDbConnection(
                      @"Provider=Microsoft.Jet.OLEDB.4.0;Data Source=" + pathOnly +
                      ";Extended Properties=\"Text;HDR=" + header + "\""))
            using (OleDbCommand command = new OleDbCommand(sql, connection))
            using (OleDbDataAdapter adapter = new OleDbDataAdapter(command))
            {
                DataTable dataTable = new DataTable();
                dataTable.Locale = CultureInfo.CurrentCulture;
                adapter.Fill(dataTable);
                
                return dataTable;
            }
        }

    }
}
