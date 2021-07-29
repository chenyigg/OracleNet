using Oracle.ManagedDataAccess.Client;
using System;
using System.Data;
using System.Threading.Tasks;

namespace OracleNet
{
    public interface IOracleHelper
    {
        Task<OracleDataReader> GetDataReaderAsync(string sql, string connStr, params OracleParameter[] param);

        Task<DataSet> GetDataSetAsync(string sql, string connStr, params OracleParameter[] param);

        Task<DataTable> GetDataTableAsync(string sql, string connStr, params OracleParameter[] param);

        Task<object> GetScalarAsync(string sql, string connStr, params OracleParameter[] param);

        Task<int> GetExecuteNonQueryAsync(string sql, string connStr, params OracleParameter[] param);

        /// <summary>
        /// 批量插入
        /// </summary>
        /// <param name="sql">sql</param>
        /// <param name="connStr">连接字符串</param>
        /// <param name="ArrayBindCount">插入数量</param>
        /// <param name="param">参数数组</param>
        /// <returns></returns>
        Task<int> BatchInsertAsync(string sql, string connStr, int ArrayBindCount, params OracleParameter[] param);

        DataTable GetDataTable(string sql, string connStr, params OracleParameter[] param);

        public int GetExecuteNonQuery(string sql, string connStr, params OracleParameter[] param);
    }

    public class OracleHelper : IOracleHelper
    {
        public OracleHelper()
        {

        }

        public async Task<OracleDataReader> GetDataReaderAsync(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    using (OracleCommand command = new OracleCommand(sql, conn))
                    {
                        if (param != null && param.Length > 0)
                        {
                            command.Parameters.AddRange(param);
                        }
                        conn.Open();
                        return await Task.FromResult<OracleDataReader>(command.ExecuteReader());
                    }
                }
                catch (Exception e)
                {
                    return await Task.FromResult<OracleDataReader>(null);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }

        }

        public async Task<DataSet> GetDataSetAsync(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    using (OracleCommand command = new OracleCommand(sql, conn))
                    {

                        if (param != null && param.Length > 0)
                        {
                            command.Parameters.AddRange(param);
                        }
                        conn.Open();
                        OracleDataAdapter dataDapter = new OracleDataAdapter(command);
                        DataSet ds = new DataSet();
                        dataDapter.Fill(ds);
                        return await Task.FromResult<DataSet>(ds);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return await Task.FromResult<DataSet>(null);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }

        }

        public async Task<DataTable> GetDataTableAsync(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    OracleCommand command = new OracleCommand(sql, conn);

                    if (param != null && param.Length > 0)
                    {
                        command.Parameters.AddRange(param);
                    }
                    conn.Open();
                    OracleDataAdapter dataDapter = new OracleDataAdapter(command);
                    DataTable dt = new DataTable();
                    dataDapter.Fill(dt);
                    return await Task.FromResult<DataTable>(dt);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return await Task.FromResult<DataTable>(null);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }
        }

        public async Task<object> GetScalarAsync(string sql, string connStr, params OracleParameter[] param)
        {
            OracleConnection conn = new OracleConnection(connStr);
            try
            {
                OracleCommand command = new OracleCommand(sql, conn);
                if (param != null && param.Length > 0)
                {
                    command.Parameters.AddRange(param);
                }
                conn.Open();
                object reulst = command.ExecuteScalar();
                return await Task.FromResult<int>(0);
            }
            catch (Exception e)
            {

                return await Task.FromResult<int>(0);
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        public async Task<int> GetExecuteNonQueryAsync(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    using (OracleCommand command = new OracleCommand(sql, conn))
                    {
                        if (param != null && param.Length > 0)
                        {
                            command.Parameters.AddRange(param);
                        }
                        conn.Open();
                        return await Task.FromResult<int>(command.ExecuteNonQuery());
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    //
                    return await Task.FromResult<int>(0);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }

        }

        public async Task<int> BatchInsertAsync(string sql, string connStr, int ArrayBindCount, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    using (OracleCommand command = new OracleCommand(sql, conn))
                    {
                        command.ArrayBindCount = ArrayBindCount;            //批量插入记录的条数，一定要赋值
                        command.CommandText = sql;
                        if (param != null && param.Length > 0)
                        {
                            command.Parameters.AddRange(param);
                        }
                        conn.Open();
                        return await Task.FromResult<int>(command.ExecuteNonQuery());
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    //
                    return await Task.FromResult<int>(0);
                }
                finally
                {
                    await conn.CloseAsync();
                }
            }


        }

        public DataTable GetDataTable(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    OracleCommand command = new OracleCommand(sql, conn);

                    if (param != null && param.Length > 0)
                    {
                        command.Parameters.AddRange(param);
                    }
                    conn.Open();
                    OracleDataAdapter dataDapter = new OracleDataAdapter(command);
                    DataTable dt = new DataTable();
                    dataDapter.Fill(dt);
                    return dt;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    return null;
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        public int GetExecuteNonQuery(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(connStr))
            {
                try
                {
                    using (OracleCommand command = new OracleCommand(sql, conn))
                    {
                        if (param != null && param.Length > 0)
                        {
                            command.Parameters.AddRange(param);
                        }
                        conn.Open();
                        return command.ExecuteNonQuery();
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e.ToString());
                    return 0;
                }
                finally
                {
                    conn.Close();
                }
            }

        }
    }
}