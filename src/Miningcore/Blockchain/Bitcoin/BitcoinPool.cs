using System.Globalization;
using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using Autofac;
using AutoMapper;
using Microsoft.IO;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.JsonRpc;
using Miningcore.Messaging;
using Miningcore.Mining;
using Miningcore.Nicehash;
using Miningcore.Notifications.Messages;
using Miningcore.Persistence;
using Miningcore.Persistence.Repositories;
using Miningcore.Stratum;
using Miningcore.Time;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NLog;
using static Miningcore.Util.ActionUtils;
using Contract = Miningcore.Contracts.Contract;

namespace Miningcore.Blockchain.Bitcoin;

[CoinFamily(CoinFamily.Bitcoin)]
public class BitcoinPool : PoolBase
{
    public BitcoinPool(IComponentContext ctx,
        JsonSerializerSettings serializerSettings,
        IConnectionFactory cf,
        IStatsRepository statsRepo,
        IMapper mapper,
        IMasterClock clock,
        IMessageBus messageBus,
        RecyclableMemoryStreamManager rmsm,
        NicehashService nicehashService,
        IMinerRepository minerRepo) :
        base(ctx, serializerSettings, cf, statsRepo, mapper, clock, messageBus, rmsm, nicehashService)
    {
        Contract.RequiresNonNull(minerRepo);

        this.minerRepo = minerRepo; 
        this.addressesCacheClearTimer = new Timer(ClearAddressesCache, null, TimeSpan.FromHours(1), TimeSpan.FromHours(1));
    }

    protected readonly IMinerRepository minerRepo;
    protected object currentJobParams;
    protected BitcoinJobManager manager;
    private BitcoinTemplate coin;
    private readonly Dictionary<string, string> addressesCache = new Dictionary<string, string>();
    private readonly Timer addressesCacheClearTimer;

    private void ClearAddressesCache(object state) {
        //Clear the cache
        addressesCache.Clear();
        logger.Info(() => $"addresses cache cleared");
    }
    
    private void Dispose() {
        //Dispose the timer when the service is disposed
        addressesCacheClearTimer.Dispose();
    }

    protected virtual async Task OnSubscribeAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest)
    {
        var request = tsRequest.Value;

        if(request.Id == null)
            throw new StratumException(StratumError.MinusOne, "missing request id");

        var context = connection.ContextAs<BitcoinWorkerContext>();
        var requestParams = request.ParamsAs<string[]>();

        var data = new object[]
        {
            new object[]
            {
                new object[] { BitcoinStratumMethods.SetDifficulty, connection.ConnectionId },
                new object[] { BitcoinStratumMethods.MiningNotify, connection.ConnectionId }
            }
        }
        .Concat(manager.GetSubscriberData(connection))
        .ToArray();

        await connection.RespondAsync(data, request.Id);

        // setup worker context
        context.IsSubscribed = true;
        context.UserAgent = requestParams.FirstOrDefault()?.Trim();

        // Nicehash support
        var nicehashDiff = await GetNicehashStaticMinDiff(context, coin.Name, coin.GetAlgorithmName());

        if(nicehashDiff.HasValue)
        {
            logger.Info(() => $"[{connection.ConnectionId}] Nicehash detected. Using API supplied difficulty of {nicehashDiff.Value}");

            context.VarDiff = null; // disable vardiff
            context.SetDifficulty(nicehashDiff.Value);
        }

        // send intial update
        await connection.NotifyAsync(BitcoinStratumMethods.SetDifficulty, new object[] { context.Difficulty });
        await connection.NotifyAsync(BitcoinStratumMethods.MiningNotify, currentJobParams);
    }

    private async Task<string> GetWorkerAddressByCredentials(string workerName, string password)
    {
        if(string.IsNullOrEmpty(workerName) || string.IsNullOrEmpty(password))
            return null;

        var poolId = poolConfig.Id;
        var passwordHash = HashingUtils.ComputeSha256Hash(password);

        var key = workerName + ":" + passwordHash;
        if (addressesCache.ContainsKey(key))
        {
            return addressesCache[key];
        }

        return await cf.RunTx(async (con, tx) =>
        {
            var address = await minerRepo.GetWorkerAddressAsync(con, tx, poolId, workerName, passwordHash);
            addressesCache.Add(key, address);

            return address;
        });
    }

    protected virtual async Task OnAuthorizeAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest, CancellationToken ct)
    {
        var request = tsRequest.Value;

        if(request.Id == null)
            throw new StratumException(StratumError.MinusOne, "missing request id");

        var context = connection.ContextAs<BitcoinWorkerContext>();
        var requestParams = request.ParamsAs<string[]>();
        var workerValue = requestParams?.Length > 0 ? requestParams[0] : null;
        var password = requestParams?.Length > 1 ? requestParams[1] : null;
        var passParts = password?.Split(PasswordControlVarsSeparator);

        // extract worker/miner
        var split = workerValue?.Split('.');
        var username = split?.FirstOrDefault()?.Trim();
        var isAddress = await manager.ValidateAddressAsync(username, ct);
        var minerName = string.Empty;
        logger.Info(() => $"[{connection.ConnectionId}] Auth request: worker [{workerValue}] password [{password}]");

        // isAddress check was created for backwards compatibility only - remove it after switching to the new approach
        if(isAddress)
        {
            minerName = username;
            logger.Info(() => $"[{connection.ConnectionId}] Auth request: minerName [{minerName}]");
            var workerName = string.Join(".", split.Skip(1))?.Trim() ?? string.Empty;
            
            // assumes that minerName is an address
            context.IsAuthorized = isAddress;
            context.Miner = minerName;
            context.Worker = workerName;
        }
        else
        {
            //valid flow: we should retrieve address from db
            minerName = await GetWorkerAddressByCredentials(username, password);
            var workerName = workerValue;

            // assumes that minerName is an address
            context.IsAuthorized = await manager.ValidateAddressAsync(minerName, ct);
            context.Miner = minerName;
            context.Worker = workerName;
        }
        
        if(context.IsAuthorized)
        {
            // respond
            await connection.RespondAsync(context.IsAuthorized, request.Id);

            // log association
            logger.Info(() => $"[{connection.ConnectionId}] Authorized worker {workerValue}");

            // extract control vars from password
            var staticDiff = GetStaticDiffFromPassparts(passParts);
            // Static diff
             if(staticDiff.HasValue &&
               (context.VarDiff != null && staticDiff.Value >= context.VarDiff.Config.MinDiff ||
                   context.VarDiff == null && staticDiff.Value > context.Difficulty))
            {
                context.VarDiff = null; // disable vardiff
                context.SetDifficulty(staticDiff.Value);

                logger.Info(() => $"[{connection.ConnectionId}] Setting static difficulty of {staticDiff.Value}");

                await connection.NotifyAsync(BitcoinStratumMethods.SetDifficulty, new object[] { context.Difficulty });
            }
        }

        else
        {
            await connection.RespondErrorAsync(StratumError.UnauthorizedWorker, "Authorization failed", request.Id, context.IsAuthorized);

            if(clusterConfig?.Banning?.BanOnLoginFailure is null or true)
            {
                // issue short-time ban if unauthorized to prevent DDos on daemon (validateaddress RPC)
                logger.Info(() => $"[{connection.ConnectionId}] Banning unauthorized worker {minerName} for {loginFailureBanTimeout.TotalSeconds} sec");

                banManager.Ban(connection.RemoteEndpoint.Address, loginFailureBanTimeout);

                Disconnect(connection);
            }
        }
    }

    protected virtual async Task OnSubmitAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest, CancellationToken ct)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<BitcoinWorkerContext>();

        try
        {
            if(request.Id == null)
                throw new StratumException(StratumError.MinusOne, "missing request id");

            // check age of submission (aged submissions are usually caused by high server load)
            var requestAge = clock.Now - tsRequest.Timestamp.UtcDateTime;

            if(requestAge > maxShareAge)
            {
                logger.Warn(() => $"[{connection.ConnectionId}] Dropping stale share submission request (server overloaded?)");
                return;
            }

            // check worker state
            context.LastActivity = clock.Now;

            // validate worker
            if(!context.IsAuthorized)
                throw new StratumException(StratumError.UnauthorizedWorker, "unauthorized worker");
            else if(!context.IsSubscribed)
                throw new StratumException(StratumError.NotSubscribed, "not subscribed");

            var requestParams = request.ParamsAs<string[]>();

            // submit
            var share = await manager.SubmitShareAsync(connection, requestParams, ct);
            await connection.RespondAsync(true, request.Id);

            // publish
            messageBus.SendMessage(new StratumShare(connection, share));

            // telemetry
            PublishTelemetry(TelemetryCategory.Share, clock.Now - tsRequest.Timestamp.UtcDateTime, true);

            logger.Info(() => $"[{connection.ConnectionId}] Share accepted: D={Math.Round(share.Difficulty * coin.ShareMultiplier, 3)}");

            // update pool stats
            if(share.IsBlockCandidate)
                poolStats.LastPoolBlockTime = clock.Now;

            // update client stats
            context.Stats.ValidShares++;

            // publish
            messageBus.SendMessage(new StratumShareStatistic(connection, BuildShareStatistic(context, share, connection)));
            
            await UpdateVarDiffAsync(connection, false, ct);
        }

        catch(StratumException ex)
        {
            // telemetry
            PublishTelemetry(TelemetryCategory.Share, clock.Now - tsRequest.Timestamp.UtcDateTime, false);

            // update client stats
            context.Stats.InvalidShares++;
            logger.Info(() => $"[{connection.ConnectionId}] Share rejected: {ex.Message} [{context.UserAgent}]");
            
            // publish
            messageBus.SendMessage(new StratumShareStatistic(connection, BuildShareStatistic(context, null, connection)));
            
            // banning
            ConsiderBan(connection, context, poolConfig.Banning);

            throw;
        }
    }

    private ShareStatistic BuildShareStatistic(BitcoinWorkerContext context, Share share, StratumConnection connection)
    {
        var workerValue = context.Worker;
        var split = workerValue?.Split('.');
        var workerName = split?.FirstOrDefault()?.Trim();
        var device = string.Join(".", split.Skip(1))?.Trim() ?? string.Empty;
        
        var shareStatistic = new ShareStatistic();
        if(share != null)
        {
            shareStatistic.PoolId = share.PoolId;
            shareStatistic.BlockHeight = share.BlockHeight;
            shareStatistic.Difficulty = share.Difficulty;
            shareStatistic.NetworkDifficulty = share.NetworkDifficulty;
            shareStatistic.Miner = share.Miner;
            shareStatistic.Worker = workerName;
            shareStatistic.Device = device;
            shareStatistic.UserAgent = share.UserAgent;
            shareStatistic.IpAddress = share.IpAddress;
            shareStatistic.Source = share.Source;
            shareStatistic.Created = share.Created;
            shareStatistic.IsValid = true;
        }
        else
        {
            shareStatistic.PoolId = poolConfig.Id;
            shareStatistic.BlockHeight = 0;
            shareStatistic.Difficulty = 0;
            shareStatistic.NetworkDifficulty = 0;
            shareStatistic.Miner = context.Miner;
            shareStatistic.Worker = workerName;
            shareStatistic.Device = device;
            shareStatistic.UserAgent = context.UserAgent;
            shareStatistic.IpAddress = connection.RemoteEndpoint.Address.ToString();
            shareStatistic.Source = clusterConfig.ClusterName;
            shareStatistic.Created = clock.Now;
            shareStatistic.IsValid = false;
        }
        
        return shareStatistic;
    }

    private async Task OnSuggestDifficultyAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<BitcoinWorkerContext>();

        // acknowledge
        await connection.RespondAsync(true, request.Id);

        try
        {
            var requestedDiff = (double) Convert.ChangeType(request.Params, TypeCode.Double)!;

            // client may suggest higher-than-base difficulty, but not a lower one
            var poolEndpoint = poolConfig.Ports[connection.LocalEndpoint.Port];

            if(requestedDiff > poolEndpoint.Difficulty)
            {
                context.SetDifficulty(requestedDiff);
                await connection.NotifyAsync(BitcoinStratumMethods.SetDifficulty, new object[] { context.Difficulty });

                logger.Info(() => $"[{connection.ConnectionId}] Difficulty set to {requestedDiff} as requested by miner");
            }
        }

        catch(Exception ex)
        {
            logger.Error(ex, () => $"Unable to convert suggested difficulty {request.Params}");
        }
    }

    private async Task OnConfigureMiningAsync(StratumConnection connection, Timestamped<JsonRpcRequest> tsRequest)
    {
        var request = tsRequest.Value;
        var context = connection.ContextAs<BitcoinWorkerContext>();

        var requestParams = request.ParamsAs<JToken[]>();
        var extensions = requestParams[0].ToObject<string[]>();
        var extensionParams = requestParams[1].ToObject<Dictionary<string, JToken>>();
        var result = new Dictionary<string, object>();

        if(extensions != null)
        {
            foreach(var extension in extensions)
            {
                switch(extension)
                {
                    case BitcoinStratumExtensions.VersionRolling:
                        ConfigureVersionRolling(connection, context, extensionParams, result);
                        break;

                    case BitcoinStratumExtensions.MinimumDiff:
                        ConfigureMinimumDiff(connection, context, extensionParams, result);
                        break;
                }
            }
        }

        await connection.RespondAsync(result, request.Id);
    }

    private void ConfigureVersionRolling(StratumConnection connection, BitcoinWorkerContext context,
        IReadOnlyDictionary<string, JToken> extensionParams, Dictionary<string, object> result)
    {
        //var requestedBits = extensionParams[BitcoinStratumExtensions.VersionRollingBits].Value<int>();
        var requestedMask = BitcoinConstants.VersionRollingPoolMask;

        if(extensionParams.TryGetValue(BitcoinStratumExtensions.VersionRollingMask, out var requestedMaskValue))
            requestedMask = uint.Parse(requestedMaskValue.Value<string>(), NumberStyles.HexNumber);

        // Compute effective mask
        context.VersionRollingMask = BitcoinConstants.VersionRollingPoolMask & requestedMask;

        // enabled
        result[BitcoinStratumExtensions.VersionRolling] = true;
        result[BitcoinStratumExtensions.VersionRollingMask] = context.VersionRollingMask.Value.ToStringHex8();

        logger.Info(() => $"[{connection.ConnectionId}] Using version-rolling mask {result[BitcoinStratumExtensions.VersionRollingMask]}");
    }

    private void ConfigureMinimumDiff(StratumConnection connection, BitcoinWorkerContext context,
        IReadOnlyDictionary<string, JToken> extensionParams, Dictionary<string, object> result)
    {
        var requestedDiff = extensionParams[BitcoinStratumExtensions.MinimumDiffValue].Value<double>();

        // client may suggest higher-than-base difficulty, but not a lower one
        var poolEndpoint = poolConfig.Ports[connection.LocalEndpoint.Port];

        if(requestedDiff > poolEndpoint.Difficulty)
        {
            context.VarDiff = null; // disable vardiff
            context.SetDifficulty(requestedDiff);

            logger.Info(() => $"[{connection.ConnectionId}] Difficulty set to {requestedDiff} as requested by miner. VarDiff now disabled.");

            // enabled
            result[BitcoinStratumExtensions.MinimumDiff] = true;
        }
    }

    protected virtual async Task OnNewJobAsync(object jobParams)
    {
        currentJobParams = jobParams;

        logger.Info(() => $"Broadcasting job {((object[]) jobParams)[0]}");

        await Guard(() => ForEachMinerAsync(async (connection, ct) =>
        {
            var context = connection.ContextAs<BitcoinWorkerContext>();

            // varDiff: if the client has a pending difficulty change, apply it now
            if(context.ApplyPendingDifficulty())
                await connection.NotifyAsync(BitcoinStratumMethods.SetDifficulty, new object[] { context.Difficulty });

            // send job
            await connection.NotifyAsync(BitcoinStratumMethods.MiningNotify, currentJobParams);
        }));
    }

    public override double HashrateFromShares(double shares, double interval)
    {
        var multiplier = BitcoinConstants.Pow2x32;
        var result = shares * multiplier / interval;

        if(coin.HashrateMultiplier.HasValue)
            result *= coin.HashrateMultiplier.Value;

        return result;
    }

    public override double ShareMultiplier => coin.ShareMultiplier;

    #region Overrides

    public override void Configure(PoolConfig pc, ClusterConfig cc)
    {
        coin = pc.Template.As<BitcoinTemplate>();

        base.Configure(pc, cc);
    }

    protected override async Task SetupJobManager(CancellationToken ct)
    {
        manager = ctx.Resolve<BitcoinJobManager>(
            new TypedParameter(typeof(IExtraNonceProvider), new BitcoinExtraNonceProvider(poolConfig.Id, clusterConfig.InstanceId)));

        manager.Configure(poolConfig, clusterConfig);

        await manager.StartAsync(ct);

        if(poolConfig.EnableInternalStratum == true)
        {
            disposables.Add(manager.Jobs
                .Select(job => Observable.FromAsync(() =>
                    Guard(()=> OnNewJobAsync(job),
                        ex=> logger.Debug(() => $"{nameof(OnNewJobAsync)}: {ex.Message}"))))
                .Concat()
                .Subscribe(_ => { }, ex =>
                {
                    logger.Debug(ex, nameof(OnNewJobAsync));
                }));

            // start with initial blocktemplate
            await manager.Jobs.Take(1).ToTask(ct);
        }

        else
        {
            // keep updating NetworkStats
            disposables.Add(manager.Jobs.Subscribe());
        }
    }

    protected override async Task InitStatsAsync(CancellationToken ct)
    {
        await base.InitStatsAsync(ct);

        blockchainStats = manager.BlockchainStats;
    }


    protected override WorkerContextBase CreateWorkerContext()
    {
        return new BitcoinWorkerContext();
    }

    protected override async Task OnRequestAsync(StratumConnection connection,
        Timestamped<JsonRpcRequest> tsRequest, CancellationToken ct)
    {
        var request = tsRequest.Value;

        try
        {
            switch(request.Method)
            {
                case BitcoinStratumMethods.Subscribe:
                    await OnSubscribeAsync(connection, tsRequest);
                    break;

                case BitcoinStratumMethods.Authorize:
                    await OnAuthorizeAsync(connection, tsRequest, ct);
                    break;

                case BitcoinStratumMethods.SubmitShare:
                    await OnSubmitAsync(connection, tsRequest, ct);
                    break;

                case BitcoinStratumMethods.SuggestDifficulty:
                    await OnSuggestDifficultyAsync(connection, tsRequest);
                    break;

                case BitcoinStratumMethods.MiningConfigure:
                    await OnConfigureMiningAsync(connection, tsRequest);
                    // ignored
                    break;

                case BitcoinStratumMethods.ExtraNonceSubscribe:
                    await connection.RespondAsync(true, request.Id);
                    break;

                case BitcoinStratumMethods.GetTransactions:
                    // ignored
                    break;

                case BitcoinStratumMethods.MiningMultiVersion:
                    // ignored
                    break;

                default:
                    logger.Debug(() => $"[{connection.ConnectionId}] Unsupported RPC request: {JsonConvert.SerializeObject(request, serializerSettings)}");

                    await connection.RespondErrorAsync(StratumError.Other, $"Unsupported request {request.Method}", request.Id);
                    break;
            }
        }

        catch(StratumException ex)
        {
            await connection.RespondErrorAsync(ex.Code, ex.Message, request.Id, false);
        }
    }

    protected override async Task OnVarDiffUpdateAsync(StratumConnection connection, double newDiff, CancellationToken ct)
    {
        await base.OnVarDiffUpdateAsync(connection, newDiff, ct);

        if(connection.Context.ApplyPendingDifficulty())
        {
            await connection.NotifyAsync(BitcoinStratumMethods.SetDifficulty, new object[] { connection.Context.Difficulty });
            await connection.NotifyAsync(BitcoinStratumMethods.MiningNotify, currentJobParams);
        }
    }

    #endregion // Overrides
}
