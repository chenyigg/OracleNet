using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data;
using System.IO;
using System.Configuration;
using Oracle.ManagedDataAccess.Client;
using Microsoft.Extensions.Configuration;
using System.Threading.Tasks;

namespace OracleNet
{
    public interface IOracleHelper
    {
        Task<OracleDataReader> GetDataReader(string sql, string connStr, params OracleParameter[] param);

        Task<DataSet> GetDataSet(string sql, string connStr, params OracleParameter[] param);

        Task<DataTable> GetDataTable(string sql, string connStr, params OracleParameter[] param);

        Task<object> GetScalar(string sql, string connStr, params OracleParameter[] param);

        Task<int> GetExecuteNonQuery(string sql, string connStr, params OracleParameter[] param);
    }

    public class OracleHelper : IOracleHelper
    {
        private IConfiguration _configuration;

        public OracleHelper()
        {

        }

        public OracleHelper(IConfiguration configuration)
        {
            _configuration = configuration;
        }

        public async Task<OracleDataReader> GetDataReader(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(_configuration[$"ConnectionStrings:{connStr}"]))
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

        public async Task<DataSet> GetDataSet(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(_configuration[$"ConnectionStrings:{connStr}"]))
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

        public async Task<DataTable> GetDataTable(string sql, string connStr, params OracleParameter[] param)
        {
            using (OracleConnection conn = new OracleConnection(_configuration[$"ConnectionStrings:{connStr}"]))
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


        public async Task<object> GetScalar(string sql, string connStr, params OracleParameter[] param)
        {
            OracleConnection conn = new OracleConnection(_configuration[$"ConnectionStrings:{connStr}"]);
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

        public async Task<int> GetExecuteNonQuery(string sql,string connStr, params OracleParameter[] param)
        {
            OracleConnection conn = new OracleConnection(_configuration[$"ConnectionStrings:{connStr}"]);
            try
            {
                OracleCommand command = new OracleCommand(sql, conn);
                if (param != null && param.Length > 0)
                {
                    command.Parameters.AddRange(param);
                }
                conn.Open();
                return await Task.FromResult<int>(command.ExecuteNonQuery());
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
}