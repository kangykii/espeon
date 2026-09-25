// Source-only V1. Paste into a new C# cBot targeting .NET 6 or later.
// API reference checked 2026-09-21; compile/demo verification remains required.
using System;
using System.IO;
using File = System.IO.File;
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Security.Cryptography;
using System.Threading.Tasks;
using cAlgo.API;
using cAlgo.API.Internals;

namespace cAlgo.Robots
{
    public class Settings
    {
        public string Profile { get; set; } = "";
        public string Model { get; set; } = "jev-1.13.0";
        public bool AllowOrders { get; set; }
        public double RiskFraction { get; set; } = .001;
        public double DailyLossFraction { get; set; } = .005;
        public double MaxDrawdownFraction { get; set; } = .02;
        public int MaxTradesPerDay { get; set; } = 4;
        public int CooldownMinutes { get; set; } = 75;
        public int HoldMinutes { get; set; } = 60;
        public int DecisionTtlSeconds { get; set; } = 20;
        public double MaxSpreadBps { get; set; } = 5;
        public double MaxSpreadAtr { get; set; } = .10;
        public double SlippageBps { get; set; } = 1;
        public double CostReserveBps { get; set; } = 10;
        public double MaxVolumeUnits { get; set; } = 0; // Must be set from broker contract before execution.
        public double MinConfidence { get; set; } = .6;
        public double MaxEntryDriftAtr { get; set; } = .15;
        public double TargetR { get; set; } = 1.5;
        public double MinStopAtr { get; set; } = .5;
        public double MaxStopAtr { get; set; } = 2;
        public int MaxCalendarAgeMinutes { get; set; } = 1440;
        public string CalendarPath { get; set; } = "calendar.json";
        public string ExpectedSymbol { get; set; } = "BTCUSD";
        public void Validate()
        {
            if (Profile!="BTC_24X7_V1" || string.IsNullOrWhiteSpace(ExpectedSymbol) || string.IsNullOrWhiteSpace(Model) || Model.Contains("latest") ||
                RiskFraction <= 0 || RiskFraction > .0025 || DailyLossFraction <= 0 ||
                DailyLossFraction > .01 || MaxDrawdownFraction <= 0 || MaxDrawdownFraction > .05 ||
                MaxTradesPerDay < 1 || MaxTradesPerDay > 8 || CooldownMinutes < 75 ||
                HoldMinutes < 45 || HoldMinutes > 90 || DecisionTtlSeconds < 1 || DecisionTtlSeconds > 30 ||
                MaxSpreadBps <= 0 || MaxSpreadAtr <= 0 || SlippageBps < 0 || CostReserveBps <= 0 ||
                MaxVolumeUnits < 0 || MinConfidence < .6 || MinConfidence > 1 ||
                MaxEntryDriftAtr <= 0 || TargetR <= 0 || MinStopAtr <= 0 || MaxStopAtr < MinStopAtr ||
                MaxCalendarAgeMinutes < 1) throw new Exception("Invalid settings");
        }
    }
    public class Secrets
    {
        public string TypeSafeApiKey { get; set; } = "";
    }    public record Candle(DateTime OpenUtc, double O, double H, double L, double C, double TickVolume);
    public record Frame(Candle[] Bars, double Atr, double Efficiency, double NetAtr,
                        double RangeLow, double RangeHigh);
    public class CalendarEvent
    {
        public string Id { get; set; } = "";
        public DateTime TimeUtc { get; set; }
        public DateTime KnownAtUtc { get; set; }
        public string Impact { get; set; } = "";
        public string Name { get; set; } = "";
    }
    public class CalendarSnapshot
    {
        public DateTime AsOfUtc { get; set; }
        public DateTime CoverageFromUtc { get; set; }
        public DateTime CoverageToUtc { get; set; }
        public string Symbol { get; set; } = "";
        public string Source { get; set; } = "";
        public CalendarEvent[] Events { get; set; } = Array.Empty<CalendarEvent>();
    }
    public class Snapshot
    {
        public string Schema { get; set; } = "snapshot-v1";
        public string Id { get; set; } = "";
        public string Symbol { get; set; } = "";
        public DateTime DecisionUtc { get; set; }
        public DateTime ObservedUtc { get; set; }
        public double Bid { get; set; }
        public double Ask { get; set; }
        public double PipSize { get; set; }
        public Frame M15 { get; set; }
        public Frame H1 { get; set; }
        public Frame H4 { get; set; }
        public double? TickVwap { get; set; }
        public double? RelativeTickActivity { get; set; }
        public string VolumeKind { get; set; } = "BROKER_TICK_COUNT";
        public string VwapKind { get; set; } = "UTC_DAY_TYPICAL_PRICE_TICK_WEIGHTED_PROXY";
        public object OrderFlow { get; set; } = null;
        public object Depth { get; set; } = null;
        public CalendarSnapshot Calendar { get; set; }
        public string CalendarError { get; set; }
        public string ConfigHash { get; set; } = "";
        public string PromptHash { get; set; } = "";
    }
    public record Candidate(string Direction, double StopAnchor, double Obstacle, double StopPips,
                            double TargetPips, string Reason);
    public class ChoiceAnswer
    {
        public string type { get; set; } = "";
        public string choice { get; set; } = "";
        public double confidence { get; set; }
        public Dictionary<string,double> probabilities { get; set; } = new();
    }
    public class ApiReply
    {
        public string model { get; set; } = "";
        public Dictionary<string,ChoiceAnswer> answers { get; set; } = new();
        public JsonElement? usage { get; set; }
    }
    public record Stage(string Request, string Raw, ApiReply Reply, DateTime StartedUtc, DateTime EndedUtc);
    public class Judgement
    {
        public string Id { get; set; } = "";
        public Stage Context { get; set; }
        public Stage Setup { get; set; }
        public string Error { get; set; }
        public string Direction { get; set; } = "NONE";
        public double Confidence { get; set; }
        public string[] Reasons { get; set; } = Array.Empty<string>();
    }
    public static class Core
    {
        public static readonly JsonSerializerOptions Json = new() { PropertyNameCaseInsensitive = false };
        public static string Serialize(object o) => JsonSerializer.Serialize(o, Json);
        public static string Hash(string s) => Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(s)));
        public static double BpsToPips(double price, double bps, double pipSize)
        {
            if(!double.IsFinite(price) || !double.IsFinite(bps) || !double.IsFinite(pipSize) || price<=0 || bps<0 || pipSize<=0)
                throw new ArgumentException("Invalid BTC price/bps/pip size");
            return price*bps/10000/pipSize;
        }
        public static bool SpreadAllowed(double bid,double ask,double atr,Settings c)
            => bid>0 && ask>=bid && atr>0 && ask-bid<=(bid+ask)/2*c.MaxSpreadBps/10000 && ask-bid<=c.MaxSpreadAtr*atr;
        public static bool ContinuousBars(Candle[] bars,int minutes)
            => bars.Skip(1).Select((b,i)=>b.OpenUtc-bars[i].OpenUtc).All(gap=>gap==TimeSpan.FromMinutes(minutes));
        public static Frame Features(Candle[] b)
        {
            if (b.Length < 20 || b.Any(x => !double.IsFinite(x.C) || !double.IsFinite(x.H) ||
                !double.IsFinite(x.L) || !double.IsFinite(x.O) || x.L <= 0 || x.H < x.L ||
                x.O < x.L || x.O > x.H || x.C < x.L || x.C > x.H || x.TickVolume < 0))
                throw new Exception("Invalid or insufficient bars");
            var tr = b.Skip(1).Select((x,i) => Math.Max(x.H-x.L,
                Math.Max(Math.Abs(x.H-b[i].C),Math.Abs(x.L-b[i].C)))).TakeLast(14).Average();
            var recent = b.TakeLast(12).ToArray();
            var path = recent.Skip(1).Select((x,i) => Math.Abs(x.C-recent[i].C)).Sum();
            if (tr <= 0 || !double.IsFinite(tr)) throw new Exception("Invalid ATR");
            return new Frame(b, tr, path > 0 ? Math.Abs(recent.Last().C-recent[0].C)/path : 0,
                (recent.Last().C-recent[0].C)/tr, recent.Min(x=>x.L), recent.Max(x=>x.H));
        }
        // Broad mechanical eligibility, not an attempt to encode every qualitative judgment.
        public static Candidate BuildCandidate(Snapshot s, Settings c)
        {
            var b=s.M15.Bars; var last=b.Last(); var prev=b[^2];
            int d=s.H1.NetAtr>0 && s.H4.NetAtr>0 ? 1 : s.H1.NetAtr<0 && s.H4.NetAtr<0 ? -1 : 0;
            string no="";
            if (d==0) no="HTF_CONFLICT";
            else if (s.H1.Efficiency<.25 || s.H4.Efficiency<.25) no="NO_DIRECTIONAL_PROGRESS";
            else if (!(d>0 ? last.C>prev.C && b.TakeLast(4).Any(x=>x.C<x.O)
                           : last.C<prev.C && b.TakeLast(4).Any(x=>x.C>x.O))) no="NO_PULLBACK_RESUMPTION";
            double entry=d>0?s.Ask:s.Bid;
            double anchor=d>0 ? b.TakeLast(4).Min(x=>x.L)-.1*s.M15.Atr
                              : b.TakeLast(4).Max(x=>x.H)+.1*s.M15.Atr;
            double distance=d*(entry-anchor);
            // Include all supplied closed higher-timeframe extremes as known opposing levels.
            var levels=s.H1.Bars.Concat(s.H4.Bars).Concat(b.Take(b.Length-4));
            var opposing=levels.Select(x=>d>0?x.H:x.L).Where(x=>d*(x-entry)>0).ToArray();
            double obstacle=opposing.Length==0 ? 0 : opposing.OrderBy(x=>d*(x-entry)).First();
            if(no=="" && (distance<c.MinStopAtr*s.M15.Atr || distance>c.MaxStopAtr*s.M15.Atr)) no="STOP_GEOMETRY";
            if(no=="" && s.TickVwap.HasValue && Math.Abs(entry-s.TickVwap.Value)>2*s.M15.Atr) no="OVEREXTENDED";
            if(no=="" && obstacle!=0 && d*(obstacle-entry)<c.TargetR*distance+.1*s.M15.Atr) no="LEVEL_OBSTRUCTION";
            return new Candidate(no==""?(d>0?"LONG":"SHORT"):"NONE",anchor,obstacle,
                Math.Max(0,distance/s.PipSize),Math.Max(0,c.TargetR*distance/s.PipSize),no);
        }
        public static string CalendarGate(CalendarSnapshot cal, string symbol, DateTime now, Settings c)
        {
            if (cal==null || cal.Events==null || cal.Symbol!=symbol || string.IsNullOrWhiteSpace(cal.Source) ||
                cal.Source.Contains("REPLACE") || cal.AsOfUtc>now || cal.AsOfUtc<now.AddMinutes(-c.MaxCalendarAgeMinutes) ||
                cal.CoverageFromUtc>now.AddMinutes(-15) || cal.CoverageToUtc<now.AddMinutes(c.HoldMinutes+30))
                return "CALENDAR_UNKNOWN";
            if(cal.Events.Any(e=>e==null || e.KnownAtUtc==default || e.KnownAtUtc>cal.AsOfUtc || e.TimeUtc==default || string.IsNullOrWhiteSpace(e.Id) || !new[]{"HIGH","MEDIUM","LOW"}.Contains(e.Impact)))
                return "CALENDAR_INVALID";
            return cal.Events.Any(e=>e.Impact=="HIGH" && e.TimeUtc>=now.AddMinutes(-15) &&
                e.TimeUtc<=now.AddMinutes(c.HoldMinutes+30)) ? "EVENT_WINDOW" : "";
        }
        public static void ValidateReply(ApiReply r, JsonElement questions, string model)
        {
            if(r==null || r.model!=model || r.answers==null) throw new Exception("MODEL_OR_SHAPE");
            var keys=questions.EnumerateObject().Select(x=>x.Name).ToArray();
            if(!r.answers.Keys.OrderBy(x=>x).SequenceEqual(keys.OrderBy(x=>x))) throw new Exception("ANSWER_KEYS");
            foreach(var k in keys)
            {
                var a=r.answers[k]; var options=questions.GetProperty(k).GetProperty("criteria")
                    .EnumerateObject().Select(x=>x.Name).ToArray();
                if(a==null || a.type!="choice" || !options.Contains(a.choice) || !double.IsFinite(a.confidence) ||
                    a.confidence<0 || a.confidence>1 || a.probabilities==null ||
                    !a.probabilities.Keys.OrderBy(x=>x).SequenceEqual(options.OrderBy(x=>x)) ||
                    a.probabilities.Values.Any(v=>!double.IsFinite(v)||v<0||v>1) ||
                    Math.Abs(a.probabilities.Values.Sum()-1)>.001 ||
                    a.probabilities[a.choice]<a.probabilities.Values.Max()-1e-6)
                    throw new Exception("ANSWER_INVALID:"+k);
            }
        }
    }
    public sealed class JevClient : IDisposable
    {
        readonly HttpClient http = new() { Timeout=TimeSpan.FromSeconds(7) };
        readonly string model;
        readonly JsonElement context, setup;
        public JevClient(string key, Settings c, string prompts)
        {
            model=c.Model;
            http.DefaultRequestHeaders.Authorization=new AuthenticationHeaderValue("Bearer", key);
            using var doc=JsonDocument.Parse(prompts);
            context=doc.RootElement.GetProperty("context").Clone(); setup=doc.RootElement.GetProperty("setup").Clone();
        }
        async Task<Stage> Call(object state, JsonElement questions)
        {
            string request=Core.Serialize(new { model, state, questions });
            var start=DateTime.UtcNow;
            using var response=await http.PostAsync("https://api.typesafe.ai/v1/systemone",
                new StringContent(request,Encoding.UTF8,"application/json")).ConfigureAwait(false);
            string raw=await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            // Preserve bad responses as well. Status validation happens after constructing Stage.
            ApiReply parsed=null;
            try { parsed=JsonSerializer.Deserialize<ApiReply>(raw,Core.Json); } catch(JsonException) { }
            if(!response.IsSuccessStatusCode) parsed=null;
            return new Stage(request,raw,parsed,start,DateTime.UtcNow);
        }
        public async Task<Judgement> Evaluate(Snapshot s)
        {
            var j=new Judgement {Id=s.Id};
            try
            {
                // Do not send operational blockers, deterministic candidate, or portfolio outcome to Jev.
                var evidence=new { s.Symbol, s.DecisionUtc, s.M15,s.H1,s.H4,s.TickVwap,
                    s.RelativeTickActivity,s.VolumeKind,s.VwapKind,s.OrderFlow,s.Depth,
                    SpreadBps=10000*(s.Ask-s.Bid)/((s.Ask+s.Bid)/2),Calendar=s.Calendar };
                j.Context=await Call(evidence,context).ConfigureAwait(false);
                Core.ValidateReply(j.Context.Reply,context,model);
                // Always evaluate setup on valid snapshots, even if context rejects: selection diagnostics.
                j.Setup=await Call(new { Market=evidence, Context=j.Context.Reply.answers },setup).ConfigureAwait(false);
                Core.ValidateReply(j.Setup.Reply,setup,model);
                var a=j.Context.Reply.answers; var b=j.Setup.Reply.answers;
                j.Confidence=b["direction"].confidence;
                j.Reasons=a.Select(x=>"CONTEXT_"+x.Key+"="+x.Value.choice)
                    .Concat(b.Select(x=>"SETUP_"+x.Key+"="+x.Value.choice)).ToArray();
                if(a["regime"].choice=="TREND" && a["participation"].choice!="WEAK" &&
                   a["bias"].confidence>=.6 && a["regime"].confidence>=.6 &&
                   a["bias"].choice==b["direction"].choice && b["direction"].choice!="NONE" &&
                   b["pullback"].choice=="ORDERLY" && b["resumption"].choice=="RESUMING" &&
                   b["timing"].choice=="TIMELY") j.Direction=b["direction"].choice;
            }
            catch(Exception ex) { j.Error=ex.GetType().Name+":"+ex.Message; }
            return j;
        }
        public void Dispose()=>http.Dispose();
    }
    public sealed class Journal : IDisposable
    {
        readonly FileStream stream;
        readonly StreamWriter writer;
        readonly string run=Guid.NewGuid().ToString("N");
        long seq;
        public Journal(string file)
        {
            stream=new FileStream(file,FileMode.Append,FileAccess.Write,FileShare.Read);
            writer=new StreamWriter(stream,new UTF8Encoding(false));
        }
        public void Put(string kind, DateTime utc, object data, bool durable=true)
        {
            writer.WriteLine(Core.Serialize(new {Schema="journal-v1",Run=run,Seq=++seq,Utc=utc,Kind=kind,Data=data}));
            if(durable) Flush();
        }
        public void Flush() { writer.Flush(); stream.Flush(true); }
        public void Dispose() { writer.Flush(); stream.Flush(true); writer.Dispose(); }
    }
    public class RiskState
    {
        public string AccountKey {get;set;}
        public DateTime Day {get;set;}
        public double DayEquity {get;set;}
        public double PeakEquity {get;set;}
        public DateTime LastEntry {get;set;}
        public int Trades {get;set;}
        public bool DailyLatched {get;set;}
        public bool GlobalLatched {get;set;}
        public string PendingIntent {get;set;}
    }
    public class Watch
    {
        public string Id {get;set;}="";
        public DateTime Start {get;set;}
        public double Bid {get;set;}
        public double Ask {get;set;}
        public double LongMfe {get;set;}
        public double LongMae {get;set;}
        public double ShortMfe {get;set;}
        public double ShortMae {get;set;}
        public int Next {get;set;}
    }
    [Robot(TimeZone=TimeZones.UTC, AccessRights=AccessRights.FullAccess)]
    public class JevPullbackBot : Robot
    {
        [Parameter("Source/config directory",DefaultValue="C:\\JevCTraderV1")]
        public string DirectoryPath {get;set;}
        [Parameter("Execution enabled",DefaultValue=false)]
        public bool ExecutionEnabled {get;set;}
        const string Label="JEV_V1";
        readonly int[] horizons={15,30,45,60,90};
        Settings cfg;
        Bars hourly,fourHourly;
        Journal log;
        FileStream lease;
        JevClient jev;
        RiskState risk;
        readonly List<Watch> watches=new();
        readonly Dictionary<int,string> closeReasons=new();
        Task<Judgement> pending;
        Snapshot pendingSnapshot;
        Candidate pendingCandidate;
        string dir,settingsHash,promptHash;
        DateTime lastQuote, lastPersist;
        bool fatal, stopping, timedOut;
        int apiFailures;
        DateTime Now=>Server.TimeInUtc;
        string P(string file)=>Path.Combine(dir,file);
        Position[] Owned()=>Positions.Where(x=>x.Label==Label && x.SymbolName==SymbolName).ToArray();
        protected override void OnStart()
        {
            try
            {
                dir=Path.GetFullPath(DirectoryPath);
                cfg=JsonSerializer.Deserialize<Settings>(File.ReadAllText(P("settings.json")),Core.Json);
                cfg.Validate();
                if(TimeFrame!=cAlgo.API.TimeFrame.Minute15 || SymbolName!=cfg.ExpectedSymbol)
                    throw new Exception("Require configured symbol on M15 time bars");
                if(Account.IsLive) throw new Exception("V1 research build refuses live accounts; demo only");
                if(RunningMode != cAlgo.API.RunningMode.RealTime) throw new Exception("Use offline replay; historical runs must never call current Jev or calendar");
                System.IO.Directory.CreateDirectory(P("runtime-btc"));
                // Single local process/instance using this directory. Dedicated account is required.
                lease=new FileStream(P("runtime-btc/instance.lock"),FileMode.OpenOrCreate,FileAccess.ReadWrite,FileShare.None);
                string accountKey=Core.Hash(Account.BrokerName+"|"+Account.Number+"|"+SymbolName);
                bool restart=File.Exists(P("runtime-btc/risk.json"));
                risk=restart?JsonSerializer.Deserialize<RiskState>(File.ReadAllText(P("runtime-btc/risk.json")),Core.Json)
                    :new RiskState {AccountKey=accountKey,Day=Now.Date,DayEquity=Account.Equity,PeakEquity=Account.Equity};
                if(risk==null || risk.AccountKey!=accountKey || risk.DayEquity<=0 || risk.PeakEquity<=0 || risk.Day>Now.Date || risk.LastEntry>Now.AddSeconds(5)) throw new Exception("Risk state identity/value mismatch; reconcile before restart");
                if(restart) File.WriteAllText(P("runtime-btc/RECONCILE.required"),"Reconcile journal, history, open positions and pending intent before removing this file.\n");
                log=new Journal(P("runtime-btc/events.jsonl"));
                string prompts=File.ReadAllText(P("prompts.json"));
                settingsHash=Core.Hash(Core.Serialize(cfg)); promptHash=Core.Hash(prompts);
                string key=LoadTypeSafeApiKey();
                if(!string.IsNullOrWhiteSpace(key)) jev=new JevClient(key,cfg,prompts);
                hourly=MarketData.GetBars(cAlgo.API.TimeFrame.Hour,SymbolName);
                fourHourly=MarketData.GetBars(cAlgo.API.TimeFrame.Hour4,SymbolName);
                // No historical decisions on start. History is used only as known context.
                foreach(var bars in new[]{Bars,hourly,fourHourly})
                    while(bars.Count<128 && bars.LoadMoreHistory()>0) { }
                Positions.Closed+=OnClosed;
                Log("START",new {Settings=cfg,settingsHash,promptHash,CodeHash=Core.Hash(File.ReadAllText(P("JevPullbackBot.cs"))),Restart=restart,
                    SymbolName,Symbol.PipSize,Symbol.TickSize,Symbol.PipValue,
                    Symbol.VolumeInUnitsMin,Symbol.VolumeInUnitsMax,Symbol.VolumeInUnitsStep,
                    Account.Currency,ExecutionEnabled,ApiConfigured=jev!=null,Symbol.MinDistanceType,Symbol.MinStopLossDistance,Symbol.MinTakeProfitDistance});
                foreach(var p in Owned()) Log("RECOVERED_POSITION",PositionData(p));
                Save(); Timer.Start(TimeSpan.FromSeconds(1));
            }
            catch(Exception e) { Print("STARTUP FAILED: "+e.Message); fatal=true; Stop(); }
        }
        string LoadTypeSafeApiKey()
        {
            // Prefer a local, ignored secrets file. Environment variables remain a fallback.
            try
            {
                var secrets=JsonSerializer.Deserialize<Secrets>(File.ReadAllText(P("secrets.json")),Core.Json);
                if(!string.IsNullOrWhiteSpace(secrets?.TypeSafeApiKey)) return secrets.TypeSafeApiKey.Trim();
            }
            catch(FileNotFoundException) { }
            catch(JsonException e) { Print("Invalid secrets.json: "+e.Message); }
            return Environment.GetEnvironmentVariable("TYPESAFE_API_KEY")?.Trim();
        }        Candle[] Closed(Bars bars, int minutes, DateTime cutoff)
        {
            var list=new List<Candle>();
            for(int i=bars.Count-1;i>=0 && list.Count<32;i--)
                if(bars.OpenTimes[i].AddMinutes(minutes)<=cutoff)
                    list.Add(new Candle(bars.OpenTimes[i],bars.OpenPrices[i],bars.HighPrices[i],
                        bars.LowPrices[i],bars.ClosePrices[i],bars.TickVolumes[i]));
            list.Reverse();
            if(list.Count<32 || list[^1].OpenUtc.AddMinutes(minutes)>cutoff ||
                cutoff-list[^1].OpenUtc.AddMinutes(minutes)>TimeSpan.FromMinutes(minutes))
                throw new Exception("INSUFFICIENT_OR_STALE_"+minutes);
            // BTC has no assumed weekend closure. Missing bars require a fresh contiguous window.
            if(!Core.ContinuousBars(list.ToArray(),minutes)) throw new Exception("BAR_GAP_"+minutes);
            return list.ToArray();
        }
        Snapshot Capture(DateTime cutoff,string id)
        {
            var m=Core.Features(Closed(Bars,15,cutoff));
            double weighted=0,volume=0; bool anchorPresent=false;
            for(int i=0;i<Bars.Count;i++)
                if(Bars.OpenTimes[i]>=cutoff.Date && Bars.OpenTimes[i].AddMinutes(15)<=cutoff)
                {
                    anchorPresent|=Bars.OpenTimes[i]==cutoff.Date;
                    weighted+=(Bars.HighPrices[i]+Bars.LowPrices[i]+Bars.ClosePrices[i])/3*Bars.TickVolumes[i];
                    volume+=Bars.TickVolumes[i];
                }
            CalendarSnapshot cal=null; string calError=null;
            try { cal=JsonSerializer.Deserialize<CalendarSnapshot>(File.ReadAllText(P(cfg.CalendarPath)),Core.Json); }
            catch(Exception e) {calError=e.GetType().Name;}
            double avg=m.Bars.SkipLast(1).TakeLast(20).Average(x=>x.TickVolume);
            return new Snapshot {Id=id,Symbol=SymbolName,DecisionUtc=cutoff,ObservedUtc=Now,
                Bid=Symbol.Bid,Ask=Symbol.Ask,PipSize=Symbol.PipSize,M15=m,
                H1=Core.Features(Closed(hourly,60,cutoff)),H4=Core.Features(Closed(fourHourly,240,cutoff)),
                TickVwap=anchorPresent && volume>0 ? weighted/volume : null,
                RelativeTickActivity=avg>0?m.Bars.Last().TickVolume/avg:null,
                Calendar=cal,CalendarError=calError,ConfigHash=settingsHash,PromptHash=promptHash};
        }
        protected override void OnBarClosed()
        {
            if(stopping || log==null) return;
            DateTime cutoff=Bars.OpenTimes.LastValue.AddMinutes(15);
            string id=SymbolName+"-"+cutoff.ToString("yyyyMMddTHHmmss")+"-btc-v1";
            try
            {
                Log("EVALUATION_STARTED",new {Id=id,DecisionUtc=cutoff,Bid=Symbol.Bid,Ask=Symbol.Ask});
                // Markouts begin at first observable quote, not an untradeable bar-close mid.
                watches.Add(new Watch {Id=id,Start=Now,Bid=Symbol.Bid,Ask=Symbol.Ask,
                    LongMae=Symbol.Bid-Symbol.Ask,ShortMae=Symbol.Bid-Symbol.Ask});
                var s=Capture(cutoff,id); var candidate=Core.BuildCandidate(s,cfg);
                Log("EVALUATION",new {Snapshot=s,Candidate=candidate,HardReasons=HardGates(s,candidate,false)});
                if(pending!=null) { Final(id,"NONE",new[]{"MODEL_BUSY"}); return; }
                if((Now-cutoff).TotalSeconds>cfg.DecisionTtlSeconds) {Final(id,"NONE",new[]{"STALE_BAR"});return;}
                if(jev==null) {Final(id,"NONE",new[]{"API_UNCONFIGURED"});return;}
                pendingSnapshot=s;pendingCandidate=candidate;timedOut=false;
                // Background work receives immutable values; it never touches any cTrader object.
                pending=jev.Evaluate(s);
            }
            catch(Exception e) { Log("EVALUATION_ERROR",new {Id=id,Error=e.Message}); Final(id,"NONE",new[]{"DATA_INVALID"}); }
        }
        List<string> HardGates(Snapshot s,Candidate c,bool execution)
        {
            var r=new List<string>();
            if(fatal) r.Add("FATAL");
            if(risk.GlobalLatched) r.Add("DRAWDOWN_LATCH");
            if(risk.DailyLatched) r.Add("DAILY_LATCH");
            if(File.Exists(P("runtime-btc/KILL"))) r.Add("MANUAL_KILL");
            if(File.Exists(P("runtime-btc/RECONCILE.required")) || risk.PendingIntent!=null) r.Add("RECONCILIATION");
            if(Positions.Count>0 || PendingOrders.Count>0) r.Add("ACCOUNT_NOT_FLAT");
            if(!Symbol.IsTradingEnabled || !Symbol.MarketHours.IsOpened()) r.Add("MARKET_CLOSED");
            if(risk.Trades>=cfg.MaxTradesPerDay) r.Add("DAILY_TRADE_LIMIT");
            if((Now-risk.LastEntry).TotalMinutes<cfg.CooldownMinutes) r.Add("COOLDOWN");
            // Calendar-day/weekend restrictions are absent; honor this broker's actual schedule.
            for(int minute=1;minute<=cfg.HoldMinutes+2;minute++)
                if(!Symbol.MarketHours.IsOpened(Now.AddMinutes(minute))) {r.Add("BROKER_CLOSURE_AHEAD");break;}
            if(execution && cfg.MaxVolumeUnits<=0) r.Add("BROKER_VOLUME_CAP_UNSET");
            if((Now-s.DecisionUtc).TotalSeconds>cfg.DecisionTtlSeconds) r.Add("STALE_DECISION");
            if(lastQuote==default || (Now-lastQuote).TotalSeconds>2 || Symbol.Bid<=0 || Symbol.Ask<Symbol.Bid) r.Add("BAD_QUOTE");
            double spread=Symbol.Ask-Symbol.Bid;
            if(!Core.SpreadAllowed(Symbol.Bid,Symbol.Ask,s.M15.Atr,cfg)) r.Add("SPREAD");
            CalendarSnapshot latestCalendar=s.Calendar;
            if(execution)
            {
                try {latestCalendar=JsonSerializer.Deserialize<CalendarSnapshot>(File.ReadAllText(P(cfg.CalendarPath)),Core.Json);}
                catch {latestCalendar=null;}
                Log("EXECUTION_CALENDAR",new {Id=s.Id,Calendar=latestCalendar});
            }
            string calendar=Core.CalendarGate(latestCalendar,SymbolName,Now,cfg);
            if(calendar!="") r.Add(calendar);
            if(c.Direction=="NONE") r.Add(c.Reason);
            if(Math.Abs((Symbol.Bid+Symbol.Ask-s.Bid-s.Ask)/2)>cfg.MaxEntryDriftAtr*s.M15.Atr) r.Add("PRICE_DRIFT");
            if(execution && (!ExecutionEnabled || !cfg.AllowOrders)) r.Add("OBSERVE_ONLY");
            return r;
        }
        protected override void OnTick()
        {
            if(log==null || stopping) return;
            lastQuote=Now;
            try
            {
                log.Put("QUOTE",Now,new {SymbolName,Bid=Symbol.Bid,Ask=Symbol.Ask},false);
                foreach(var w in watches.ToArray())
                {
                    double lr=Symbol.Bid-w.Ask, sr=w.Bid-Symbol.Ask;
                    w.LongMfe=Math.Max(w.LongMfe,lr); w.LongMae=Math.Min(w.LongMae,lr);
                    w.ShortMfe=Math.Max(w.ShortMfe,sr); w.ShortMae=Math.Min(w.ShortMae,sr);
                    while(w.Next<horizons.Length && Now>=w.Start.AddMinutes(horizons[w.Next]))
                    {
                        int h=horizons[w.Next++]; var lag=(Now-w.Start.AddMinutes(h)).TotalSeconds;
                        Log("FORWARD",new {w.Id,HorizonMinutes=h,ObservationUtc=Now,LagSeconds=lag,
                            Valid=lag<=30,LongPriceReturn=lr,ShortPriceReturn=sr,
                            LongBps=10000*lr/w.Ask,ShortBps=10000*sr/w.Bid,
                            w.LongMfe,w.LongMae,w.ShortMfe,w.ShortMae});
                    }
                    if(w.Next==horizons.Length) watches.Remove(w);
                }
            }
            catch(Exception e) {Fail("QUOTE_LOG:"+e.Message);}
        }
        protected override void OnTimer()
        {
            if(stopping || cfg==null || risk==null) return;
            try
            {
                if(risk.Day!=Now.Date)
                {
                    risk.Day=Now.Date;risk.DayEquity=Account.Equity;risk.Trades=0;risk.DailyLatched=false;
                    Save();Log("DAY_RESET",risk);
                }
                risk.PeakEquity=Math.Max(risk.PeakEquity,Account.Equity);
                risk.DailyLatched|=Account.Equity<=risk.DayEquity*(1-cfg.DailyLossFraction);
                risk.GlobalLatched|=Account.Equity<=risk.PeakEquity*(1-cfg.MaxDrawdownFraction);
                ManagePositions();
                if(pending!=null)
                {
                    if(!timedOut && (Now-pendingSnapshot.DecisionUtc).TotalSeconds>cfg.DecisionTtlSeconds)
                    {timedOut=true;Final(pendingSnapshot.Id,"NONE",new[]{"MODEL_TIMEOUT"});apiFailures++;}
                    if(pending.IsCompleted)
                    {
                        var j=pending.GetAwaiter().GetResult();
                        Log("JEV",j);
                        if(!timedOut) Complete(pendingSnapshot,pendingCandidate,j);
                        pending=null;pendingSnapshot=null;pendingCandidate=null;
                    }
                }
                if(apiFailures>=3) {risk.GlobalLatched=true;Log("KILL",new {Reason="THREE_API_FAILURES"});apiFailures=0;}
                if((Now-lastPersist).TotalSeconds>=5) {Save();log.Flush();lastPersist=Now;}
            }
            catch(Exception e) {Fail("TIMER:"+e.Message);}
        }
        void Complete(Snapshot s,Candidate candidate,Judgement j)
        {
            if(j.Error!=null) apiFailures++; else apiFailures=0;
            risk.DailyLatched|=Account.Equity<=risk.DayEquity*(1-cfg.DailyLossFraction);
            risk.GlobalLatched|=Account.Equity<=risk.PeakEquity*(1-cfg.MaxDrawdownFraction);
            var reasons=HardGates(s,candidate,true);
            if(j.Error!=null) reasons.Add("JEV_ERROR");
            if(j.Direction=="NONE" || j.Direction!=candidate.Direction) reasons.Add("JEV_VETO");
            if(j.Confidence<cfg.MinConfidence) reasons.Add("LOW_CONFIDENCE");
            if(reasons.Count>0) {Final(s.Id,"NONE",reasons.ToArray());return;}
            int sign=candidate.Direction=="LONG"?1:-1;
            var type=sign>0?TradeType.Buy:TradeType.Sell;
            double quote=sign>0?Symbol.Ask:Symbol.Bid;
            double slippagePips=Core.BpsToPips(quote,cfg.SlippageBps,Symbol.PipSize);
            double costReservePips=Core.BpsToPips(quote,cfg.CostReserveBps,Symbol.PipSize);
            double stopPips=sign*(quote-candidate.StopAnchor)/Symbol.PipSize;
            double worstStop=stopPips+slippagePips;
            double distanceScale=Symbol.MinDistanceType==SymbolMinDistanceType.Pips ? 1 :
                Symbol.MinDistanceType==SymbolMinDistanceType.Percentage ? quote/100/Symbol.PipSize : double.NaN;
            if(!double.IsFinite(distanceScale) || worstStop<Symbol.MinStopLossDistance*distanceScale ||
                cfg.TargetR*worstStop<Symbol.MinTakeProfitDistance*distanceScale)
            {Final(s.Id,"NONE",new[]{"BROKER_MIN_DISTANCE"});return;}
            if(stopPips*Symbol.PipSize<cfg.MinStopAtr*s.M15.Atr || worstStop*Symbol.PipSize>cfg.MaxStopAtr*s.M15.Atr ||
                (candidate.Obstacle!=0 && sign*(candidate.Obstacle-quote)<
                  (cfg.TargetR*worstStop+slippagePips)*Symbol.PipSize+.1*s.M15.Atr))
            {Final(s.Id,"NONE",new[]{"RECHECK_GEOMETRY"});return;}
            // Risk uses worst allowed stop distance plus round-trip cost/slippage reserve.
            double budget=Account.Equity*cfg.RiskFraction;
            double volume=Symbol.VolumeForFixedRisk(budget,worstStop+costReservePips,RoundingMode.Down);
            volume=Symbol.NormalizeVolumeInUnits(Math.Min(volume,Math.Min(cfg.MaxVolumeUnits,Symbol.VolumeInUnitsMax)),RoundingMode.Down);
            if(!double.IsFinite(volume) || volume<Symbol.VolumeInUnitsMin ||
                Symbol.AmountRisked(volume,worstStop+costReservePips)>budget*1.001 ||
                Symbol.GetEstimatedMargin(type,volume)>Account.FreeMargin*.5)
            {Final(s.Id,"NONE",new[]{"SIZE_OR_MARGIN"});return;}
            risk.PendingIntent=s.Id; Save(); // Write-ahead fence: never retry an ambiguous submission.
            Log("ORDER_INTENT",new {Id=s.Id,Direction=candidate.Direction,Quote=quote,Bid=Symbol.Bid,Ask=Symbol.Ask,
                Volume=volume,StopPips=worstStop,TargetPips=cfg.TargetR*worstStop,Budget=budget,SlippagePips=slippagePips,CostReservePips=costReservePips});
            var result=ExecuteMarketRangeOrder(type,SymbolName,volume,slippagePips,quote,Label,
                worstStop,cfg.TargetR*worstStop,s.Id);
            if(!result.IsSuccessful)
            {
                // Broker errors can be ambiguous. Require reconciliation even if no position is visible.
                risk.GlobalLatched=true;Save();
                Log("ORDER_ERROR",new {Id=s.Id,Error=result.Error?.ToString()});
                Final(s.Id,"NONE",new[]{"ORDER_ERROR_RECONCILE"});return;
            }
            var p=result.Position;
            risk.LastEntry=p.EntryTime;risk.Trades++;risk.PendingIntent=null;Save();
            Log("FILL",new {Id=s.Id,Position=PositionData(p),RequestedQuote=quote,
                SlippagePips=sign*(p.EntryPrice-quote)/Symbol.PipSize,Bid=Symbol.Bid,Ask=Symbol.Ask});
            Final(s.Id,candidate.Direction,Array.Empty<string>());
            if(!p.StopLoss.HasValue || !p.TakeProfit.HasValue ||
                sign*(p.EntryPrice-p.StopLoss.Value)<=0 || sign*(p.TakeProfit.Value-p.EntryPrice)<=0 ||
                Symbol.AmountRisked(p.VolumeInUnits,Math.Abs(p.EntryPrice-p.StopLoss.Value)/Symbol.PipSize+costReservePips)>budget*1.01)
            {risk.GlobalLatched=true; Save(); RequestClose(p,"PROTECTION_OR_RISK_MISMATCH");}
        }
        void ManagePositions()
        {
            foreach(var p in Owned())
            {
                string why=fatal || risk.GlobalLatched ? "GLOBAL_KILL" : risk.DailyLatched ? "DAILY_KILL" :
                    File.Exists(P("runtime-btc/KILL")) ? "MANUAL_KILL" :
                    !p.StopLoss.HasValue ? "MISSING_STOP" :
                    Now>=p.EntryTime.AddMinutes(cfg.HoldMinutes) ? "TIME_EXIT" :
                    !Symbol.MarketHours.IsOpened(Now.AddMinutes(2)) ? "BROKER_CLOSURE_EXIT" : "";
                if(why!="") RequestClose(p,why);
            }
        }
        void RequestClose(Position p,string reason)
        {
            closeReasons[p.Id]=reason;
            // An audit failure must never prevent attempting a risk-reducing close.
            try {Log("EXIT_INTENT",new {p.Id,Reason=reason,Bid=Symbol.Bid,Ask=Symbol.Ask});} catch { }
            var result=ClosePosition(p);
            if(!result.IsSuccessful) {Print("Close failed "+p.Id+":"+result.Error);risk.GlobalLatched=true;}
        }
        object PositionData(Position p)=>new {p.Id,p.Comment,p.SymbolName,Direction=p.TradeType.ToString(),
            p.EntryTime,p.EntryPrice,p.VolumeInUnits,p.StopLoss,p.TakeProfit,p.GrossProfit,p.NetProfit,p.Commissions,p.Swap};
        void OnClosed(PositionClosedEventArgs e)
        {
            if(e.Position.Label!=Label || e.Position.SymbolName!=SymbolName) return;
            try
            {
                var p=e.Position;
                // History supplies actual closing prices, not a quote guessed from the event.
                var deals=History.Where(h=>h.PositionId==p.Id).Select(h=>new {h.PositionId,h.EntryTime,
                    h.ClosingTime,h.EntryPrice,h.ClosingPrice,h.VolumeInUnits,h.GrossProfit,h.NetProfit,h.Commissions,h.Swap}).ToArray();
                Log("EXIT",new {Id=p.Comment,Position=PositionData(p),BrokerReason=e.Reason.ToString(),
                    RequestedReason=closeReasons.TryGetValue(p.Id,out var why)?why:null,Deals=deals,
                    HistoryPending=deals.Length==0,Bid=Symbol.Bid,Ask=Symbol.Ask});
                closeReasons.Remove(p.Id);Save();
            }
            catch(Exception ex) {Fail("EXIT_LOG:"+ex.Message);}
        }
        void Final(string id,string direction,string[] reasons)=>Log("FINAL",new {Id=id,Direction=direction,Reasons=reasons});
        void Log(string kind,object value)
        {
            try {log.Put(kind,Now,value);} catch {fatal=true;throw;}
        }
        void Save()
        {
            string file=P("runtime-btc/risk.json");string temp=file+".tmp";
            using(var f=new FileStream(temp,FileMode.Create,FileAccess.Write,FileShare.None))
            {var bytes=Encoding.UTF8.GetBytes(Core.Serialize(risk));f.Write(bytes,0,bytes.Length);f.Flush(true);}
            File.Move(temp,file,true);
        }
        void Fail(string why)
        {
            fatal=true;Print("FAIL CLOSED: "+why);
            if(risk!=null) risk.GlobalLatched=true;
            try {File.WriteAllText(P("runtime-btc/KILL"),why);Save();} catch { }
            try {ManagePositions();} catch(Exception e) {Print("Emergency close failed: "+e.Message);}
        }
        protected override void OnStop()
        {
            stopping=true;
            if(log!=null)
            {
                foreach(var p in Owned()) RequestClose(p,"BOT_STOP");
                try
                {
                    if(pendingSnapshot!=null && !timedOut) Final(pendingSnapshot.Id,"NONE",new[]{"BOT_STOP"});
                    foreach(var w in watches) Log("FORWARD_CENSORED",new {w.Id,Reason="BOT_STOP",NextHorizon=horizons[w.Next]});
                    Log("STOP",new {OpenOwned=Owned().Length});Save();log.Dispose();
                } catch(Exception e) {Print("Stop audit failure: "+e.Message);}
            }
            jev?.Dispose();lease?.Dispose();
        }
    }
}
