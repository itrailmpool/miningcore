using System.Data;
using AutoMapper;
using Dapper;
using Miningcore.Persistence.Model;
using Miningcore.Persistence.Repositories;

namespace Miningcore.Persistence.Postgres.Repositories;

public class MinerRepository : IMinerRepository
{
    public MinerRepository(IMapper mapper)
    {
        this.mapper = mapper;
    }

    private readonly IMapper mapper;

    public async Task<MinerSettings> GetSettingsAsync(IDbConnection con, IDbTransaction tx, string poolId, string workerName)
    {
        const string query = @"SELECT * FROM miner_settings WHERE poolid = @poolId AND workerName = @workerName";

        var entity = await con.QuerySingleOrDefaultAsync<Entities.MinerSettings>(query, new {poolId, workerName}, tx);

        return mapper.Map<MinerSettings>(entity);
    }

    public Task UpdateSettingsAsync(IDbConnection con, IDbTransaction tx, MinerSettings settings)
    {
        const string query = @"INSERT INTO miner_settings(poolid, workername, password, address, paymentthreshold, created, updated)
            VALUES(@poolid, @workername, @password, @address, @paymentthreshold, now(), now())
            ON CONFLICT ON CONSTRAINT miner_settings_pkey DO UPDATE
            SET paymentthreshold = @paymentthreshold, updated = now()
            WHERE miner_settings.poolid = @poolid AND miner_settings.workername = @workername";

        return con.ExecuteAsync(query, settings, tx);
    }

    public async Task<string> GetWorkerAddressAsync(IDbConnection con, IDbTransaction tx, string poolId, string workerName, string password)
    {
        const string query = @"SELECT address FROM miner_settings WHERE poolid = @poolid AND workername = @workerName AND password = @password";

        return await con.QuerySingleOrDefaultAsync<string>(query, new { poolId, workerName, password });
    }
}
