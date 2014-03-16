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
        private string HighwayPath = "c:\\incoming\\highways.csv";
        private string StationPath = "c:\\incoming\\freeway_stations.csv";
        private string DetectorPath = "c:\\incoming\\freeway_detectors.csv";
        private string LoopPath = "c:\\incoming\\freeway_loopdata.csv";
        static void Main(string[] args)
        {
            Program p = new Program();

            //Create database context and connect to Keyspace
            FluentCassandra.Connections.Server svr = new FluentCassandra.Connections.Server(Properties.Settings.Default.CassandraIP);
            FluentCassandra.CassandraSession session = new CassandraSession(Properties.Settings.Default.KeySpace,svr);
            var db = new CassandraContext(session);

            Console.WriteLine("Time Started: " + DateTime.Now.ToString());
            
            //INSERT DATA!!!
            p.insertHighwayStations(db);
            p.InsertStationLoops(db);

            Console.WriteLine("Time Finished: " + DateTime.Now.ToString());   
            Console.Read();
        }

        /// <summary>
        /// Inserts data into LoopData column family 
        /// </summary>
        /// <param name="db">Cassandra database context</param>
        private void InsertStationLoops(CassandraContext db)
        {
            //Read in detectors
            DataTable detectors = GetDataTableFromCsv(DetectorPath, true);
            //Read in loop data
            List<LoopData> loop = getLoopData();

            //Join the loops and detectors
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
                                    StartSecond = table2.StartSecond,
                                    DayOfWeek = table2.DayOfWeek,
                                    Volume = table2.Volume,
                                    Speed = table2.Speed
                                };

            //foreach record, insert into cassandra
            foreach (var item in LoopDetectors)
            {
                string query = "INSERT INTO \"" + Properties.Settings.Default.KeySpace + "\".\"LoopData\" ( \"StationID\",\"HighwayID\",\"DetectorID\", \"StartDate\",\"StartTime\",\"StartHour\",\"DayOfWeek\",\"Volume\",\"Speed\",\"StartMinute\",\"StartSecond\") ";
                query += "VALUES( " + item.StationID.ToString() + ", " + item.HighwayID.ToString() + ", " + item.DetectorID.ToString() + ", '" + item.StartDate + "', '" + item.StartTime + "', " + item.StartHour + ", '" + item.DayOfWeek + "'," + item.Volume + "," + item.Speed + "," + item.StartMinute + "," + item.StartSecond + ")";

                db.ExecuteNonQuery(query);
            }
        }

        /// <summary>
        /// Gets loop data fro CSV file
        /// </summary>
        /// <returns>returns List of LoopData objects read from CSV file</returns>
        private List<LoopData> getLoopData() 
        {
            List<LoopData> loop = new List<LoopData>();

            using (StreamReader reader = new StreamReader(LoopPath))
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

                    }
                }
            }

            return loop;
        
        }
        /// <summary>
        /// Resolves day of week enumeration into textual representation
        /// </summary>
        /// <param name="theDay">enumeration of DayOfWeek</param>
        /// <returns>textual representation of the day of week.</returns>
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

        public void insertHighwayStations(CassandraContext db) {

            //Get highway data
            DataTable Highway = GetDataTableFromCsv(HighwayPath, true);
            //Get station data
            DataTable Stations = GetDataTableFromCsv(StationPath, true, "stationclass = 1");

            //Join highways and stations
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
            //For each tuple resulting from the join, insert into cassandra. 
            foreach (var item in HighwayStations)
            {
                string query = "INSERT INTO \"" +  Properties.Settings.Default.KeySpace  + "\".\"Stations\" ( \"StationID\",\"HighwayID\",\"DownstreamStationID\", \"LocationText\",\"LengthMid\",\"ShortDirection\",\"HighwayName\") ";
                query += "VALUES( " + item.StationID.ToString() + ", " + item.HighwayID.ToString() + ", " + item.DownstreamStationID.ToString() + ", '" + item.LocationText + "', " + item.LengthMid.ToString() + ", '" + item.ShortDirection + "', '" + item.HighwayName + "');";

                db.ExecuteNonQuery(query);
            }
        }


        static DataTable GetDataTableFromCsv(string path, bool isFirstRowHeader, string filter)
        {
            string header = isFirstRowHeader ? "Yes" : "No";

            string pathOnly = Path.GetDirectoryName(path);
            string fileName = Path.GetFileName(path);
            string sql = @"SELECT * FROM [" + fileName + "]";
            if (filter != null)
            {
               sql = sql + " WHERE " + filter;
            }
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

            return GetDataTableFromCsv(path, isFirstRowHeader, null);
        }

    }
}
