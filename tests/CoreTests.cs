using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using cAlgo.Robots;

static class CoreTests
{
    static void Check(bool ok,string reason) {if(!ok) throw new Exception(reason);}
    static void Throws(Action f,string reason)
    {try {f();} catch {return;} throw new Exception(reason);}
    static void Main()
    {
        var cfg=new Settings {Profile="BTC_24X7_V1"}; cfg.Validate();
        cfg.MinConfidence=.599;Throws(()=>cfg.Validate(),"Confidence floor must fail");cfg.MinConfidence=.6;
        cfg.CooldownMinutes=60;Throws(()=>cfg.Validate(),"At most one/hour is not strictly less than one/hour");cfg.CooldownMinutes=75;
        Check(Core.BpsToPips(100000,1,.01)==1000,"BTC bps convert through broker pip size");
        Check(Core.BpsToPips(100000,1,1)==10,"Different pip conventions preserve price allowance");
        Check(Core.SpreadAllowed(99995,100005,200,cfg),"Valid BTC spread");
        Check(!Core.SpreadAllowed(99900,100100,200,cfg),"Wide BTC spread blocked");
        var weekend=new[]{new Candle(new DateTime(2026,9,19,23,45,0),1,1,1,1,1),
            new Candle(new DateTime(2026,9,20,0,0,0),1,1,1,1,1)};
        Check(Core.ContinuousBars(weekend,15),"Saturday-to-Sunday midnight must be continuous");
        weekend[1]=weekend[1] with {OpenUtc=weekend[1].OpenUtc.AddMinutes(15)};
        Check(!Core.ContinuousBars(weekend,15),"A weekend does not excuse a missing BTC bar");
        var now=new DateTime(2026,9,21,10,0,0,DateTimeKind.Utc);
        var cal=new CalendarSnapshot {Symbol="BTCUSD",Source="fixture",AsOfUtc=now.AddHours(-1),
            CoverageFromUtc=now.AddDays(-1),CoverageToUtc=now.AddDays(1),
            Events=new[]{new CalendarEvent {Id="e",KnownAtUtc=now.AddHours(-2),TimeUtc=now.AddMinutes(90),Impact="HIGH"}}};
        Check(Core.CalendarGate(cal,"BTCUSD",now,cfg)=="EVENT_WINDOW","Holding overlap boundary must block");
        cal.Events[0].TimeUtc=now.AddMinutes(91);
        Check(Core.CalendarGate(cal,"BTCUSD",now,cfg)=="","Outside inclusive window should pass");
        cal.AsOfUtc=now.AddSeconds(1);
        Check(Core.CalendarGate(cal,"BTCUSD",now,cfg)=="CALENDAR_UNKNOWN","Future calendar must block");
        using var doc=JsonDocument.Parse("{\"direction\":{\"criteria\":{\"LONG\":\"x\",\"NONE\":\"y\"}}}");
        var reply=new ApiReply {model=cfg.Model,answers=new(){["direction"]=new ChoiceAnswer {
            type="choice",choice="LONG",confidence=.6,probabilities=new(){{"LONG",.8},{"NONE",.2}}}};
        Core.ValidateReply(reply,doc.RootElement,cfg.Model);
        reply.answers["direction"].probabilities["NONE"]=.5;
        Throws(()=>Core.ValidateReply(reply,doc.RootElement,cfg.Model),"Bad probability sum must fail");
        reply.answers["direction"].probabilities["NONE"]=.2;
        reply.model="unrequested-version";
        Throws(()=>Core.ValidateReply(reply,doc.RootElement,cfg.Model),"Model mismatch must fail");
        var bars=Enumerable.Range(0,32).Select(i=>new Candle(now.AddMinutes(15*i),100+i,102+i,99+i,101+i,100)).ToArray();
        var frame=Core.Features(bars);
        Check(frame.Efficiency==1 && frame.Atr>0 && frame.NetAtr>0,"Monotonic path representation");
        bars[31]=bars[31] with { C=double.NaN };
        Throws(()=>Core.Features(bars),"Nonfinite bar must fail");
        Console.WriteLine("Core invariants passed (no broker/API calls).");
    }
}
