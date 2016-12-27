using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Clique.Clauses
{
    public class Save
    {
        public string Column { get; set; }
        public Object Value { get; set; }

        public Save(string column, Object value)
        {
            this.Column = column;
            this.Value = value;
        }
    }
}
