using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CassandraETL.Model
{
    class LoopData
    {
        public int DetectorID { get; set; }
        public string StartDate { get; set; }
        public string StartTime { get; set; }
        public byte StartHour { get; set; }
        public byte StartMinute { get; set; }
        public byte StartSecond { get; set; }
        public string DayOfWeek { get; set; }
        public int Volume { get; set; }
        public int Speed { get; set; } 
    }
}
