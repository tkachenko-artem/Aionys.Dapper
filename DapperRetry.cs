using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;
using AnyRetry;
using Dapper;
using Microsoft.Extensions.Logging;

namespace Aionys.Dapper
{
    public class DapperRetry
    {
        private const int DefaultRetryLimit = 5;

        private readonly TimeSpan _retryEvery = TimeSpan.FromSeconds(25);
        private readonly ILogger _logger;
        private readonly int _retryLimit;

        public DapperRetry(int retryLimit = DefaultRetryLimit)
        {
            _retryLimit = retryLimit;
        }

        public DapperRetry(ILogger logger, int retryLimit = DefaultRetryLimit)
        {
            _logger = logger;
            _retryLimit = retryLimit;
        }

        #region QueryAsync<T>
        public async Task<IEnumerable<T>> QueryAsync<T>(
            string connectionString,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(QueryNewConnectionAsync<T>(connectionString, sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }

        public async Task<IEnumerable<T>> QueryAsync<T>(
            IDbConnection db,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(db.QueryAsync<T>(sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }
        #endregion

        #region QueryFirstOrDefaultAsync<T>
        public async Task<T> QueryFirstOrDefaultAsync<T>(
            string connectionString,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(QueryFirstOrDefaultNewConnectionAsync<T>(connectionString, sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }

        public async Task<T> QueryFirstOrDefaultAsync<T>(
            IDbConnection db,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(db.QueryFirstOrDefaultAsync<T>(sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }
        #endregion

        #region ExecuteAsync
        public async Task<int> ExecuteAsync(
            string connectionString,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(ExecuteNewConnectionAsync(connectionString, sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }

        public async Task<int> ExecuteAsync(
            IDbConnection db,
            string sql,
            object parameters = null,
            int? retryLimit = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            return await RetryActionAsync(db.ExecuteAsync(sql, parameters, transaction, commandTimeout, commandType), retryLimit);
        }
        #endregion

        public async Task<T> RetryActionAsync<T>(Task<T> task, int? retryLimit = null)
        {
            T result = default;

            await Retry.DoAsync(async (retryIteration, maxRetryCount) =>
            {
                result = await task;
            }, _retryEvery, retryLimit ?? _retryLimit, onFailure: ((exception, retryIteration, maxRetryCount) =>
            {
                _logger?.LogWarning(exception, $"Dapper retry #: {retryIteration + 1}");
            }));

            return result;
        }

        public async Task<int> ExecuteNewConnectionAsync(
            string connectionString,
            string sql,
            object parameters = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.ExecuteAsync(sql, parameters, transaction, commandTimeout, commandType);
            }
        }

        public async Task<T> QueryFirstOrDefaultNewConnectionAsync<T>(
            string connectionString,
            string sql,
            object parameters = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryFirstOrDefaultAsync<T>(sql, parameters, transaction, commandTimeout, commandType);
            }
        }

        private async Task<IEnumerable<T>> QueryNewConnectionAsync<T>(
            string connectionString,
            string sql,
            object parameters = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CommandType? commandType = null)
        {
            using (IDbConnection db = new SqlConnection(connectionString))
            {
                return await db.QueryAsync<T>(sql, parameters, transaction, commandTimeout, commandType);
            }
        }
    }
}