using System.Data;
using Miningcore.Persistence.Model;
using Miningcore.Persistence.Model.Projections;

namespace Miningcore.Persistence.Repositories;

public interface IShareStatisticRepository
{
    Task BatchInsertAsync(IDbConnection con, IDbTransaction tx, IEnumerable<ShareStatistic> shares, CancellationToken ct);
}
