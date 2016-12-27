using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Clique.Clauses
{
    public class Where
    {
        public string Column { get; set; }
        public string Comparator { get; set; }
        public Object Value { get; set; }
        public bool IsOR { get; set; }

        public string Concenator
        {
            get
            {
                if (this.IsOR == true)
                {
                    return "OR";
                }
                else
                {
                    return "AND";
                }
            }
        }

        public Where(string column, string comparator, Object value, bool isOR = false)
        {
            this.Column = column;
            this.Comparator = comparator;
            this.Value = value;
            this.IsOR = isOR;
        }
    }
}
