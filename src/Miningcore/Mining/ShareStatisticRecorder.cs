using System.Data.Common;
using System.Net.Sockets;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Reactive.Threading.Tasks;
using System.Text;
using AutoMapper;
using Microsoft.Extensions.Hosting;
using Miningcore.Configuration;
using Miningcore.Extensions;
using Miningcore.Messaging;
using Miningcore.Notifications.Messages;
using Miningcore.Persistence;
using Miningcore.Persistence.Repositories;
using Newtonsoft.Json;
using NLog;
using Polly;
using Polly.CircuitBreaker;
using Contract = Miningcore.Contracts.Contract;
using ShareStatistic = Miningcore.Blockchain.ShareStatistic;
using static Miningcore.Util.ActionUtils;

namespace Miningcore.Mining;

/// <summary>
/// Asynchronously persist shares produced by all pools for processing by coin-specific payment processor(s)
/// </summary>
public class ShareStatisticRecorder : BackgroundService
{
    public ShareStatisticRecorder(IConnectionFactory cf,
        IMapper mapper,
        JsonSerializerSettings jsonSerializerSettings,
        IShareStatisticRepository shareStatisticRepo,
        ClusterConfig clusterConfig,
        IMessageBus messageBus)
    {
        Contract.RequiresNonNull(cf);
        Contract.RequiresNonNull(mapper);
        Contract.RequiresNonNull(shareStatisticRepo);
        Contract.RequiresNonNull(jsonSerializerSettings);
        Contract.RequiresNonNull(messageBus);

        this.cf = cf;
        this.mapper = mapper;
        this.jsonSerializerSettings = jsonSerializerSettings;
        this.messageBus = messageBus;
        this.clusterConfig = clusterConfig;

        this.shareStatisticRepo = shareStatisticRepo;

        pools = clusterConfig.Pools.ToDictionary(x => x.Id, x => x);

        BuildFaultHandlingPolicy();
        ConfigureRecovery();
    }

    private static readonly ILogger logger = LogManager.GetCurrentClassLogger();
    private readonly IShareStatisticRepository shareStatisticRepo;
    private readonly IConnectionFactory cf;
    private readonly JsonSerializerSettings jsonSerializerSettings;
    private readonly IMessageBus messageBus;
    private readonly ClusterConfig clusterConfig;
    private readonly Dictionary<string, PoolConfig> pools;
    private readonly IMapper mapper;

    private IAsyncPolicy faultPolicy;
    private bool hasLoggedPolicyFallbackFailure;
    private string recoveryFilename;
    private const int RetryCount = 3;
    private const string PolicyContextKeyShares = "shareStatistic";
    private bool notifiedAdminOnPolicyFallback = false;

    private async Task PersistSharesAsync(IList<ShareStatistic> shares)
    {
        var context = new Dictionary<string, object> { { PolicyContextKeyShares, shares } };

        await faultPolicy.ExecuteAsync(ctx => PersistSharesCoreAsync((IList<ShareStatistic>) ctx[PolicyContextKeyShares]), context);
    }

    private async Task PersistSharesCoreAsync(IList<ShareStatistic> shares)
    {
        logger.Info(() => "Online PersistSharesCoreAsync");
        await cf.RunTx(async (con, tx) =>
        {
            logger.Info(() => "Online PersistSharesCoreAsync run");
            // Insert shares
            var mapped = shares.Select(mapper.Map<Persistence.Model.ShareStatistic>).ToArray();
            await shareStatisticRepo.BatchInsertAsync(con, tx, mapped, CancellationToken.None);
        });
    }

    private static void OnPolicyRetry(Exception ex, TimeSpan timeSpan, int retry, object context)
    {
        logger.Warn(() => $"Retry {retry} in {timeSpan} due to {ex.Source}: {ex.GetType().Name} ({ex.Message})");
    }

    private Task OnPolicyFallbackAsync(Exception ex, Context context)
    {
        logger.Warn(() => $"Fallback due to {ex.Source}: {ex.GetType().Name} ({ex.Message})");
        return Task.CompletedTask;
    }

    private async Task OnExecutePolicyFallbackAsync(Context context, CancellationToken ct)
    {
        var shares = (IList<ShareStatistic>) context[PolicyContextKeyShares];

        try
        {
            await using(var stream = new FileStream(recoveryFilename, FileMode.Append, FileAccess.Write))
            {
                await using(var writer = new StreamWriter(stream, new UTF8Encoding(false)))
                {
                    if(stream.Length == 0)
                        WriteRecoveryFileheader(writer);

                    foreach(var share in shares)
                    {
                        var json = JsonConvert.SerializeObject(share, jsonSerializerSettings);
                        await writer.WriteLineAsync(json);
                    }
                }
            }

            NotifyAdminOnPolicyFallback();
        }

        catch(Exception ex)
        {
            if(!hasLoggedPolicyFallbackFailure)
            {
                logger.Fatal(ex, "Fatal error during policy fallback execution. Share(s) will be lost!");
                hasLoggedPolicyFallbackFailure = true;
            }
        }
    }

    private static void WriteRecoveryFileheader(TextWriter writer)
    {
        writer.WriteLine("# The existence of this file means shares could not be committed to the database.");
        writer.WriteLine("# You should stop the pool cluster and run the following command:");
        writer.WriteLine("# miningcore -c <path-to-config> -rs <path-to-this-file>\n");
    }

    public async Task RecoverSharesAsync(string filename)
    {
        logger.Info(() => $"Recovering shares using {filename} ...");

        try
        {
            var successCount = 0;
            var failCount = 0;
            const int bufferSize = 100;

            await using(var stream = new FileStream(filename, FileMode.Open, FileAccess.Read))
            {
                using(var reader = new StreamReader(stream, new UTF8Encoding(false)))
                {
                    var shares = new List<ShareStatistic>();
                    var lastProgressUpdate = DateTime.UtcNow;

                    while(!reader.EndOfStream)
                    {
                        var line = await reader.ReadLineAsync();

                        if(string.IsNullOrEmpty(line))
                            continue;

                        // skip blank lines
                        line = line.Trim();

                        if(line.Length == 0)
                            continue;

                        // skip comments
                        if(line.StartsWith("#"))
                            continue;

                        // parse
                        try
                        {
                            var share = JsonConvert.DeserializeObject<ShareStatistic>(line, jsonSerializerSettings);
                            shares.Add(share);
                        }

                        catch(JsonException ex)
                        {
                            logger.Error(ex, () => $"Unable to parse share record: {line}");
                            failCount++;
                        }

                        // import
                        try
                        {
                            if(shares.Count == bufferSize)
                            {
                                await PersistSharesCoreAsync(shares);

                                successCount += shares.Count;
                                shares.Clear();
                            }
                        }

                        catch(Exception ex)
                        {
                            logger.Error(ex, () => "Unable to import shares");
                            failCount++;
                        }

                        // progress
                        var now = DateTime.UtcNow;

                        if(now - lastProgressUpdate > TimeSpan.FromSeconds(10))
                        {
                            logger.Info($"{successCount} shares imported");
                            lastProgressUpdate = now;
                        }
                    }

                    // import remaining shares
                    try
                    {
                        if(shares.Count > 0)
                        {
                            await PersistSharesCoreAsync(shares);

                            successCount += shares.Count;
                        }
                    }

                    catch(Exception ex)
                    {
                        logger.Error(ex, () => "Unable to import shares");
                        failCount++;
                    }
                }
            }

            if(failCount == 0)
                logger.Info(() => $"Successfully imported {successCount} shares");
            else
                logger.Warn(() => $"Successfully imported {successCount} shares with {failCount} failures");
        }

        catch(FileNotFoundException)
        {
            logger.Error(() => $"Recovery file {filename} was not found");
        }
    }

    private void NotifyAdminOnPolicyFallback()
    {
        if(clusterConfig.Notifications?.Admin?.Enabled == true &&
           clusterConfig.Notifications?.Admin?.NotifyPaymentSuccess == true &&
           !notifiedAdminOnPolicyFallback)
        {
            notifiedAdminOnPolicyFallback = true;

            messageBus.SendMessage(new AdminNotification("ShareStatistic Recorder Policy Fallback",
                $"The ShareStatistic Recorder's Policy Fallback has been engaged. Check ShareStatistic recovery file {recoveryFilename}."));
        }
    }

    private void ConfigureRecovery()
    {
        recoveryFilename = !string.IsNullOrEmpty(clusterConfig.ShareStatisticRecoveryFile)
            ? clusterConfig.ShareStatisticRecoveryFile
            : "recovered-shares-statistic.txt";
    }

    private void BuildFaultHandlingPolicy()
    {
        // retry with increasing delay (1s, 2s, 4s etc)
        var retry = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .WaitAndRetryAsync(RetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                OnPolicyRetry);

        // after retries failed several times, break the circuit and fall through to
        // fallback action for one minute, not attempting further retries during that period
        var breaker = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .CircuitBreakerAsync(2, TimeSpan.FromMinutes(1));

        var fallback = Policy
            .Handle<DbException>()
            .Or<SocketException>()
            .Or<TimeoutException>()
            .FallbackAsync(OnExecutePolicyFallbackAsync, OnPolicyFallbackAsync);

        var fallbackOnBrokenCircuit = Policy
            .Handle<BrokenCircuitException>()
            .FallbackAsync(OnExecutePolicyFallbackAsync, (ex, context) => Task.CompletedTask);

        faultPolicy = Policy.WrapAsync(
            fallbackOnBrokenCircuit,
            Policy.WrapAsync(fallback, breaker, retry));
    }

    protected override Task ExecuteAsync(CancellationToken ct)
    {
        logger.Info(() => "Online ShareStatisticRecorder");

        return messageBus.Listen<StratumShareStatistic>()
            .ObserveOn(TaskPoolScheduler.Default)
            .Where(x => x.ShareStatistic != null)
            .Select(x => x.ShareStatistic)
            .Buffer(TimeSpan.FromSeconds(5), 1000)
            .Where(shares => shares.Any())
            .Select(shares => Observable.FromAsync(() =>
                Guard(() =>
                        PersistSharesAsync(shares),
                    ex => logger.Error(ex))))
            .Concat()
            .ToTask(ct)
            .ContinueWith(task =>
            {
                if(task.IsFaulted)
                    logger.Fatal(() => $"Terminated due to error {task.Exception?.InnerException ?? task.Exception}");
                else
                    logger.Info(() => "Offline");
            }, ct);
    }
}
