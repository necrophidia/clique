using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Clique.Clauses
{
    public class Join
    {
        public string FirstTableName { get; set; }
        public string FirstTableColumn { get; set; }
        public string SecondTableName { get; set; }
        public string SecondTableColumn { get; set; }

        public Join(string firstTableName, string firstTableColumn, string secondTableName, string secondTableColumn)
        {
            this.FirstTableName = firstTableName;
            this.FirstTableColumn = firstTableColumn;
            this.SecondTableName = secondTableName;
            this.SecondTableColumn = secondTableColumn;
        }
    }
}
