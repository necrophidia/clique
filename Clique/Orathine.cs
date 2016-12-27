using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.OracleClient;

namespace Clique
{
    public class Orathine
    {
        private string ConnectionString { get; set; }
        private OracleConnection Connection { get; set; }

        /// <summary>
        /// Constructor sets the connection string and opens the database connection.
        /// </summary>
        /// <param name="connectionString">Connection string to be used.</param>
        public Orathine(string connectionString)
        {
            this.ConnectionString = connectionString;
            this.Connection = new OracleConnection(connectionString);

            try
            {
                this.Connection.Open();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        /// <summary>
        /// Closes the database connection.
        /// </summary>
        public void Dispose()
        {
            try
            {
                this.Connection.Close();
            }
            catch (Exception e)
            {
                throw e;
            }
        }

        /// <summary>
        /// Executes given query string.
        /// </summary>
        /// <param name="queryString">Query string to be executed.</param>
        public int Execute(string queryString)
        {
            int result = 0;

            using (var command = this.Connection.CreateCommand())
            {
                command.CommandType = CommandType.Text;
                command.CommandText = queryString;
                using (var transaction = this.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    try
                    {
                        command.Transaction = transaction;
                        //System.IO.File.AppendAllText(@"C:\inetpub\wwwroot\Debug\OrathineQueryExecute.txt", DateTime.Now.ToString() + " | " + queryString + "\r\n");
                        result = command.ExecuteNonQuery();
                        transaction.Commit();
                    }
                    catch (OracleException ex)
                    {
                        transaction.Rollback();
                        // You can use Console Write or System IO for debugging purpose
                        // Console.WriteLine(ex.Message);
                        //System.IO.File.WriteAllText(@"C:\inetpub\wwwroot\Debug\OrathineErrorExecute.txt", ex.Message);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Insert row to table in database.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="newValues">List of save clauses consists of column name and values.</param>
        /// <param name="returningColumnName">Column name's value to be returned after save.</param>
        public string Insert(string tableName, List<Clauses.Save> newValues, string returningColumnName)
        {
            string result = null;

            if (newValues.Count > 0 && !String.IsNullOrEmpty(tableName))
            {
                using (var command = this.Connection.CreateCommand())
                {
                    string queryString = @"INSERT INTO " + tableName.ToUpper();

                    string insertColumns = " ";
                    string insertValues = " ";

                    for (int jj = 0; jj < newValues.Count; jj++)
                    {
                        var newValue = newValues[jj];

                        if (jj == 0)
                        {
                            insertColumns += @"(";
                            insertValues += @"VALUES(";
                        }
                        else if (jj > 0)
                        {
                            insertColumns += @", ";
                            insertValues += @", ";
                        }

                        string value = Convert.ToString(newValue.Value);
                        if (string.IsNullOrEmpty(value))
                        {
                            insertValues += @"null";
                        }
                        else
                        {
                            if (newValue.Value.GetType() == typeof(DateTime))
                            {
                                value = Orathine.ConvertToOracleDateFormat((DateTime)newValue.Value);
                            }
                            insertValues += @"'" + value + @"'";
                        }

                        insertColumns += newValue.Column.ToUpper();

                        if (jj == (newValues.Count - 1))
                        {
                            insertColumns += @")";
                            insertValues += @")";
                        }
                    }

                    queryString += insertColumns + insertValues;

                    queryString += @" RETURNING " + returningColumnName + " INTO :resultID";

                    var resultID = new OracleParameter(":resultID", OracleType.VarChar);
                    resultID.Size = 50;
                    resultID.Direction = ParameterDirection.Output;

                    command.Parameters.Add(resultID);

                    command.CommandType = CommandType.Text;
                    command.CommandText = queryString;

                    using (var transaction = this.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            command.Transaction = transaction;
                            //System.IO.File.AppendAllText(@"C:\inetpub\wwwroot\Debug\OrathineQueryInsert.txt", DateTime.Now.ToString() + " | " + queryString + "\r\n");
                            int rowAffected = command.ExecuteNonQuery();
                            transaction.Commit();
                            result = resultID.Value.ToString();
                        }
                        catch (OracleException ex)
                        {
                            transaction.Rollback();
                            // You can use Console Write or System IO for debugging purpose
                            // Console.WriteLine(ex.Message);
                            //System.IO.File.WriteAllText(@"C:\inetpub\wwwroot\Debug\OrathineErrorInsert.txt", ex.Message);
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Update row in table in database.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="newValues">List of save clauses consists of column name and values.</param>
        /// <param name="clauses">List of where clauses as clause statement.</param>
        public string Update(string tableName, List<Clauses.Save> newValues, List<Clauses.Where> clauses = null)
        {
            string result = null;

            if (newValues.Count > 0 && !String.IsNullOrEmpty(tableName))
            {
                using (var command = this.Connection.CreateCommand())
                {
                    string queryString = @"UPDATE " + tableName.ToUpper();

                    string updateStatement = " SET ";

                    for (int jj = 0; jj < newValues.Count; jj++)
                    {
                        var newValue = newValues[jj];

                        if (jj > 0)
                        {
                            updateStatement += @", ";
                        }

                        string value = Convert.ToString(newValue.Value);
                        if (string.IsNullOrEmpty(value))
                        {
                            updateStatement += newValue.Column.ToUpper() + @"=null";
                        }
                        else
                        {
                            if (newValue.Value.GetType() == typeof(DateTime))
                            {
                                value = Orathine.ConvertToOracleDateFormat((DateTime)newValue.Value);
                            }
                            updateStatement += newValue.Column.ToUpper() + @"='" + value + @"'";
                        }
                    }

                    queryString += updateStatement;

                    if (clauses != null && clauses.Count > 0)
                    {

                        string whereClause = " WHERE ";

                        for (int ii = 0; ii < clauses.Count; ii++)
                        {
                            var clause = clauses[ii];

                            if (ii > 0)
                            {
                                whereClause += @" " + clause.Concenator;
                            }


                            if (clause.Comparator.ToLower().Contains("in"))
                            {
                                if (clause.Value.GetType() == typeof(string[]))
                                {
                                    var rawValues = clause.Value as string[];
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator.ToUpper() + " ('" + String.Join("','", rawValues) + @"')";
                                }
                                else
                                {
                                    string value = Convert.ToString(clause.Value);
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator +
                                                    @" " + value + @"";
                                }
                            }
                            else
                            {
                                string value = Convert.ToString(clause.Value);
                                if (clause.Value.GetType() == typeof(DateTime))
                                {
                                    value = Orathine.ConvertToOracleDateFormat((DateTime)clause.Value);
                                }
                                whereClause += @" lower(" + clause.Column.ToUpper() + @") " + clause.Comparator +
                                                @" lower('" + value + @"')";
                            }
                        }

                        queryString += whereClause;
                    }

                    command.CommandType = CommandType.Text;
                    command.CommandText = queryString;

                    using (var transaction = this.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            command.Transaction = transaction;
                            //System.IO.File.AppendAllText(@"C:\inetpub\wwwroot\Debug\OrathineQueryUpdate.txt", DateTime.Now.ToString() + @" | " + queryString + "\r\n");
                            int rowAffected = command.ExecuteNonQuery();
                            transaction.Commit();
                            result = rowAffected.ToString();
                        }
                        catch (OracleException ex)
                        {
                            transaction.Rollback();
                            // You can use Console Write or System IO for debugging purpose
                            // Console.WriteLine(ex.Message);
                            //System.IO.File.WriteAllText(@"C:\inetpub\wwwroot\Debug\OrathineErrorUpdate.txt", ex.Message);
                        }

                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Delete row in table in database.
        /// </summary>
        /// <param name="tableName">Table name.</param>
        /// <param name="clauses">List of where clauses as clause statement.</param>
        public int Delete(string tableName, List<Clauses.Where> clauses = null)
        {
            int result = 0;

            if (!String.IsNullOrEmpty(tableName))
            {
                using (var command = this.Connection.CreateCommand())
                {
                    string queryString = @"DELETE FROM " + tableName.ToUpper();


                    if (clauses != null && clauses.Count > 0)
                    {
                        string whereClause = " WHERE ";

                        for (int ii = 0; ii < clauses.Count; ii++)
                        {
                            var clause = clauses[ii];

                            if (ii > 0)
                            {
                                whereClause += @" " + clause.Concenator;
                            }


                            if (clause.Comparator.ToLower().Contains("in"))
                            {
                                if (clause.Value.GetType() == typeof(string[]))
                                {
                                    var rawValues = clause.Value as string[];
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator.ToUpper() + " ('" + String.Join("','", rawValues) + @"')";
                                }
                                else
                                {
                                    string value = Convert.ToString(clause.Value);
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator +
                                                    @" " + value + @"";
                                }
                            }
                            else
                            {
                                string value = Convert.ToString(clause.Value);
                                if (clause.Value.GetType() == typeof(DateTime))
                                {
                                    value = Orathine.ConvertToOracleDateFormat((DateTime)clause.Value);
                                }
                                whereClause += @" lower(" + clause.Column.ToUpper() + @") " + clause.Comparator +
                                                @" lower('" + value + @"')";
                            }
                        }

                        queryString += whereClause;
                    }

                    command.CommandType = CommandType.Text;
                    command.CommandText = queryString;

                    using (var transaction = this.Connection.BeginTransaction(IsolationLevel.ReadCommitted))
                    {
                        try
                        {
                            command.Transaction = transaction;
                            //System.IO.File.AppendAllText(@"C:\inetpub\wwwroot\Debug\OrathineQueryDelete.txt", DateTime.Now.ToString() + " | " + queryString + "\r\n");
                            int rowAffected = command.ExecuteNonQuery();
                            transaction.Commit();
                            result = rowAffected;
                        }
                        catch (OracleException ex)
                        {
                            transaction.Rollback();
                            // You can use Console Write or System IO for debugging purpose
                            // Console.WriteLine(ex.Message);
                            //System.IO.File.WriteAllText(@"C:\inetpub\wwwroot\Debug\OrathineErrorDelete.txt", ex.Message);
                        }
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Select row from table in database.
        /// </summary>
        /// <param name="tableNames">Table names.</param>
        /// <param name="columnNames">List of column names to be selected.</param>
        /// <param name="limit">Number of rows to be selected.</param>
        /// <param name="startRow">Starting row to be selected.</param>
        /// <param name="clauses">List of where clauses as clause statement.</param>
        /// <param name="joins">List of join clauses as join statement.</param>
        /// <param name="groupColumn">Group selected rows by column(s) name(s).</param>
        /// <param name="orderColumn">Sort selected rows by column(s) name(s).</param>
        /// <param name="orderIsDesc">Sort selected rows descendingly.</param>
        public List<Dictionary<string, Object>> Select(string tableNames, string[] columnNames, int limit = 0, int startRow = 0,
                                                    List<Clauses.Where> clauses = null, List<Clauses.Join> joins = null, string groupColumn = null,
                                                    string orderColumn = null, bool orderIsDesc = false)
        {
            var result = new List<Dictionary<string, Object>>();

            if (columnNames.Length > 0 && !String.IsNullOrEmpty(tableNames))
            {
                using (var command = this.Connection.CreateCommand())
                {
                    string queryString = @"SELECT " + (String.Join(", ", columnNames)).ToUpper();
                    queryString += @" FROM " + tableNames.ToUpper();

                    if (joins != null)
                    {
                        string joinedClauses = "";
                        for (int ll = 0; ll < joins.Count; ll++)
                        {
                            var joinClause = joins[ll];
                            joinedClauses += @" LEFT JOIN ";
                            joinedClauses += joinClause.FirstTableName;
                            joinedClauses += @" ON ";
                            joinedClauses += joinClause.FirstTableName + @"." + joinClause.FirstTableColumn;
                            joinedClauses += @"=";
                            joinedClauses += joinClause.SecondTableName + @"." + joinClause.SecondTableColumn;

                            if (ll < (joins.Count - 1))
                            {
                                joinedClauses += @" ";
                            }

                            joinedClauses += " ";
                        }
                        queryString += joinedClauses.ToLower();
                    }

                    if (clauses != null && clauses.Count > 0)
                    {
                        string whereClause = " WHERE ";

                        for (int ii = 0; ii < clauses.Count; ii++)
                        {
                            var clause = clauses[ii];

                            if (ii > 0)
                            {
                                whereClause += @" " + clause.Concenator;
                            }


                            if (clause.Comparator.ToLower().Contains("in"))
                            {
                                if (clause.Value.GetType() == typeof(string[]))
                                {
                                    var rawValues = clause.Value as string[];
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator.ToUpper() + " ('" + String.Join("','", rawValues) + @"')";
                                }
                                else
                                {
                                    string value = Convert.ToString(clause.Value);
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator +
                                                    @" " + value + @"";
                                }
                            }
                            else
                            {
                                string value = Convert.ToString(clause.Value);
                                if (clause.Value.GetType() == typeof(DateTime))
                                {
                                    value = Orathine.ConvertToOracleDateFormat((DateTime)clause.Value);
                                }
                                if (clause.Comparator.ToLower().Contains("like") && value.ToLower().Contains("where") && value.ToLower().Contains("select"))
                                {
                                    whereClause += @" " + clause.Column.ToUpper() + @" " + clause.Comparator +
                                                    @" " + value + @"";
                                }
                                else
                                {
                                    whereClause += @" lower(" + clause.Column.ToUpper() + @") " + clause.Comparator +
                                                    @" lower('" + value + @"')";
                                }
                            }
                        }

                        queryString += whereClause;
                    }

                    if (limit > 0)
                    {
                        if (clauses == null)
                        {
                            queryString += @" WHERE ";
                        }
                        else
                        {
                            queryString += @" AND ";
                        }
                        queryString += @" ROWNUM <= " + (limit * (startRow + 1));
                    }

                    if (startRow > 0)
                    {
                        if (clauses == null && limit <= 0)
                        {
                            queryString += @" WHERE ";
                        }
                        else
                        {
                            queryString += @" AND ";
                        }
                        queryString += @" ROWNUM >= " + (limit * (startRow - 1));
                    }

                    if (!String.IsNullOrEmpty(groupColumn))
                    {
                        queryString += @" GROUP BY " + groupColumn;
                    }

                    if (!String.IsNullOrEmpty(orderColumn))
                    {
                        queryString += @" ORDER BY " + orderColumn;
                    }

                    if (orderIsDesc == true)
                    {
                        queryString += @" DESC";
                    }

                    command.CommandType = CommandType.Text;
                    command.CommandText = queryString;

                    try
                    {
                        //System.IO.File.AppendAllText(@"C:\inetpub\wwwroot\Debug\OrathineQuerySelect.txt", DateTime.Now.ToString() + " | " + queryString + "\r\n");

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var rowData = new Dictionary<string, Object>(StringComparer.InvariantCultureIgnoreCase);
                                for (int ll = 0; ll < columnNames.Length; ll++)
                                {
                                    string columnName = columnNames[ll].ToUpper().Replace("DISTINCT", "").Trim();
                                    rowData.Add(columnName, reader.GetValue(ll));
                                }
                                result.Add(rowData);
                            }
                        }
                    }
                    catch (OracleException ex)
                    {
                        // You can use Console Write or System IO for debugging purpose
                        // Console.WriteLine(ex.Message);
                        //System.IO.File.WriteAllText(@"C:\inetpub\wwwroot\Debug\OrathineErrorSelect.txt", ex.Message);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Convert a DateTime value to Oracle date format.
        /// </summary>
        /// <param name="rawValue">DateTime value to be converted.</param>
        public static string ConvertToOracleDateFormat(DateTime rawValue)
        {
            var formatProvider = new System.Globalization.CultureInfo("en-US", true);
            return rawValue.ToString("dd-MMM-yyyy", formatProvider);
        }
    }
}
