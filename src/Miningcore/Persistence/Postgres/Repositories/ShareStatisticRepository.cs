using System.Data;
using AutoMapper;
using Dapper;
using Miningcore.Persistence.Model;
using Miningcore.Persistence.Model.Projections;
using Miningcore.Persistence.Repositories;
using Npgsql;
using NpgsqlTypes;

namespace Miningcore.Persistence.Postgres.Repositories;

public class ShareStatisticRepository : IShareStatisticRepository
{
    public ShareStatisticRepository(IMapper mapper)
    {
        this.mapper = mapper;
    }

    private static readonly ILogger logger = LogManager.GetCurrentClassLogger();
    private readonly IMapper mapper;

    public async Task BatchInsertAsync(IDbConnection con, IDbTransaction tx, IEnumerable<ShareStatistic> shares, CancellationToken ct)
    {
        // NOTE: Even though the tx parameter is completely ignored here,
        // the COPY command still honors a current ambient transaction

        var pgCon = (NpgsqlConnection) con;
        logger.Info(() => "ShareStatisticRepository batch insert");

        const string query = @"COPY shares_statistic (poolid, blockheight, difficulty,
            networkdifficulty, miner, worker, useragent, ipaddress, source, created, isvalid, device) FROM STDIN (FORMAT BINARY)";

        await using(var writer = await pgCon.BeginBinaryImportAsync(query, ct))
        {
            foreach(var share in shares)
            {
                await writer.StartRowAsync(ct);

                await writer.WriteAsync(share.PoolId, ct);
                await writer.WriteAsync((long) share.BlockHeight, NpgsqlDbType.Bigint, ct);
                await writer.WriteAsync(share.Difficulty, NpgsqlDbType.Double, ct);
                await writer.WriteAsync(share.NetworkDifficulty, NpgsqlDbType.Double, ct);
                await writer.WriteAsync(share.Miner, ct);
                await writer.WriteAsync(share.Worker, ct);
                await writer.WriteAsync(share.UserAgent, ct);
                await writer.WriteAsync(share.IpAddress, ct);
                await writer.WriteAsync(share.Source, ct);
                await writer.WriteAsync(share.Created, NpgsqlDbType.TimestampTz, ct);
                await writer.WriteAsync(share.IsValid, NpgsqlDbType.Boolean, ct);
                await writer.WriteAsync(share.Device, ct);
            }

            await writer.CompleteAsync(ct);
        }
    }
}
