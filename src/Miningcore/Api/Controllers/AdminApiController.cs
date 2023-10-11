using Autofac;
using Microsoft.AspNetCore.Mvc;
using Miningcore.Extensions;
using Miningcore.Mining;
using Miningcore.Persistence.Repositories;
using Miningcore.Util;
using System.Collections.Concurrent;
using System.Net;
using NLog;
using Miningcore.Blockchain;

namespace Miningcore.Api.Controllers;

[Route("api/admin")]
[ApiController]
public class AdminApiController : ApiControllerBase
{
    public AdminApiController(IComponentContext ctx) : base(ctx)
    {
        gcStats = ctx.Resolve<Responses.AdminGcStats>();
        minerRepo = ctx.Resolve<IMinerRepository>();
        pools = ctx.Resolve<ConcurrentDictionary<string, IMiningPool>>();
        paymentsRepo = ctx.Resolve<IPaymentRepository>();
        balanceRepo = ctx.Resolve<IBalanceRepository>();
    }

    private readonly IPaymentRepository paymentsRepo;
    private readonly IBalanceRepository balanceRepo;
    private readonly IMinerRepository minerRepo;
    private readonly ConcurrentDictionary<string, IMiningPool> pools;

    private readonly Responses.AdminGcStats gcStats;

    private static readonly ILogger logger = LogManager.GetCurrentClassLogger();

    #region Actions

    [HttpGet("stats/gc")]
    public ActionResult<Responses.AdminGcStats> GetGcStats()
    {
        gcStats.GcGen0 = GC.CollectionCount(0);
        gcStats.GcGen1 = GC.CollectionCount(1);
        gcStats.GcGen2 = GC.CollectionCount(2);
        gcStats.MemAllocated = FormatUtil.FormatCapacity(GC.GetTotalMemory(false));

        return gcStats;
    }

    [HttpPost("forcegc")]
    public ActionResult<string> ForceGc()
    {
        GC.Collect(2, GCCollectionMode.Forced);
        return "Ok";
    }

    [HttpGet("pools/{poolId}/miners/{address}/getbalance")]
    public async Task<decimal> GetMinerBalanceAsync(string poolId, string address)
    {
        return await cf.Run(con => balanceRepo.GetBalanceAsync(con, poolId, address));
    }

    [HttpGet("pools/{poolId}/miners/{worker}/settings")]
    public async Task<Responses.MinerSettings> GetMinerSettingsAsync(string poolId, string worker)
    {
        var pool = GetPool(poolId);

        if(string.IsNullOrEmpty(worker))
            throw new ApiException("Invalid or missing worker name", HttpStatusCode.NotFound);

        var result = await cf.Run(con=> minerRepo.GetSettingsAsync(con, null, pool.Id, worker));

        if(result == null)
            throw new ApiException("No settings found", HttpStatusCode.NotFound);

        return mapper.Map<Responses.MinerSettings>(result);
    }

    [HttpPost("pools/{poolId}/miners/{worker}/settings")]
    public async Task<Requests.MinerSettings> SetMinerSettingsAsync(string poolId, string worker,
      [FromBody] Requests.AddMinerRequest request, CancellationToken ct)
    {
        var pool = GetPool(poolId);

        if(string.IsNullOrEmpty(worker))
            throw new ApiException("Invalid or missing worker name", HttpStatusCode.NotFound);

        if(request?.Settings == null)
            throw new ApiException("Invalid or missing settings", HttpStatusCode.BadRequest);

        // map settings
        var mapped = mapper.Map<Persistence.Model.MinerSettings>(request.Settings);

        // clamp limit
        if(pool.PaymentProcessing != null)
            mapped.PaymentThreshold = Math.Max(mapped.PaymentThreshold, pool.PaymentProcessing.MinimumPayment);

        mapped.PoolId = pool.Id;
        mapped.WorkerName = worker;
        mapped.Password = HashingUtils.ComputeSha256Hash(mapped.Password);

        // finally update the settings
        return await cf.RunTx(async (con, tx) =>
        {
            await minerRepo.UpdateSettingsAsync(con, tx, mapped);

            logger.Info(() => $"Updated settings for pool {pool.Id}, worker name {mapped.WorkerName}");

            var result = await minerRepo.GetSettingsAsync(con, tx, mapped.PoolId, mapped.WorkerName);
            return mapper.Map<Requests.MinerSettings>(result);
        });
    }
    #endregion // Actions
}
