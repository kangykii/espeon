"""Offline replay and audit tools. Python 3.11+ standard library. Not run by author.
No network or order APIs. See VALIDATION.md for estimator limitations.
"""
import argparse
import bisect
import datetime as dt
import hashlib
import json
import math
import random
import sqlite3
from collections import defaultdict
from pathlib import Path

UTC = dt.timezone.utc


def stamp(s):
    x = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    return x.replace(tzinfo=UTC) if x.tzinfo is None else x.astimezone(UTC)


def load(file):
    rows = []
    with open(file, encoding="utf-8-sig") as f:
        for n, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
                if row["Schema"] != "journal-v1": raise ValueError("Schema mismatch")
                row["_time"] = stamp(row["Utc"])
                rows.append(row)
            except Exception as e:
                raise ValueError(f"Invalid journal line {n}; quarantine, never silently skip") from e
    rows.sort(key=lambda r: (r["_time"], r["Run"], r["Seq"]))
    keys = [(r["Run"], r["Seq"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate event identity")
    return rows


def ingest(rows, database):
    with sqlite3.connect(database) as db:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("CREATE TABLE IF NOT EXISTS events(run TEXT,seq INTEGER,utc TEXT,kind TEXT,id TEXT,payload TEXT,sha256 TEXT,PRIMARY KEY(run,seq))")
        db.execute("CREATE INDEX IF NOT EXISTS by_evaluation ON events(id,kind)")
        for r in rows:
            data = r["Data"]
            identifier = data.get("Id") or data.get("Snapshot", {}).get("Id")
            payload = json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)
            digest = hashlib.sha256(payload.encode()).hexdigest()
            old = db.execute("SELECT sha256 FROM events WHERE run=? AND seq=?", (r["Run"], r["Seq"])).fetchone()
            if old and old[0] != digest:
                raise ValueError("Immutable event changed")
            db.execute("INSERT OR IGNORE INTO events VALUES(?,?,?,?,?,?,?)",
                       (r["Run"], r["Seq"], r["Utc"], r["Kind"], identifier, payload, digest))


class Tape:
    def __init__(self, rows):
        q = [r for r in rows if r["Kind"] == "QUOTE"]
        self.t = [r["_time"] for r in q]
        self.q = [(r["Data"]["Bid"], r["Data"]["Ask"]) for r in q]
        if any(not (math.isfinite(b) and math.isfinite(a) and 0 < b <= a) for b, a in self.q):
            raise ValueError("Invalid quote")

    def after(self, when, tolerance=30):
        i = bisect.bisect_left(self.t, when)
        return i if i < len(self.t) and (self.t[i]-when).total_seconds() <= tolerance else None

    def covered(self, first, last):
        return all((self.t[k]-self.t[k-1]).total_seconds() <= 30 for k in range(first+1,last+1))


def index(rows):
    evaluations, judgments, starts = {}, {}, []
    for r in rows:
        if r["Kind"] == "START": starts.append(r)
        elif r["Kind"] == "EVALUATION":
            key = r["Data"]["Snapshot"]["Id"]
            if key in evaluations: raise ValueError("Repeated evaluation ID; reconcile restart data")
            evaluations[key] = r
        elif r["Kind"] == "JEV":
            key = r["Data"]["Id"]
            if key in judgments: raise ValueError("Repeated model result")
            judgments[key] = r
    if not starts: raise ValueError("Missing configuration provenance")
    if len({r["Data"]["settingsHash"] for r in starts}) != 1 or len({r["Data"]["promptHash"] for r in starts}) != 1:
        raise ValueError("Split experiments at config/prompt changes")
    return sorted(evaluations.values(),key=lambda r:r["_time"]),judgments,starts[0]["Data"]["Settings"]


def markouts(es, js, tape):
    out=[]
    for e in es:
        s=e["Data"]["Snapshot"]; start=stamp(s["ObservedUtc"]); first=tape.after(start)
        for h in (15,30,45,60,90):
            last=tape.after(start+dt.timedelta(minutes=h))
            valid=first is not None and last is not None and tape.covered(first,last)
            for side,sign in (("LONG",1),("SHORT",-1)):
                row=dict(id=s["Id"],horizon=h,side=side,valid=valid,
                         reason=None if valid else "MISSING_OR_GAPPED_TAPE")
                if valid:
                    entry=s["Ask"] if sign==1 else s["Bid"]
                    path=[sign*((b if sign==1 else a)-entry) for b,a in tape.q[first:last+1]]
                    row.update(price_return=path[-1],return_bps=10000*path[-1]/entry,
                               mfe=max(0,max(path)),mae=min(0,min(path)),
                               lag_seconds=(tape.t[last]-start).total_seconds()-60*h)
                out.append(row)
    return out


EXOGENOUS={"SESSION","STALE_DECISION","BAD_QUOTE","SPREAD","CALENDAR_UNKNOWN",
           "CALENDAR_INVALID","EVENT_WINDOW","PRICE_DRIFT","MARKET_CLOSED","BROKER_CLOSURE_AHEAD"}


def price_costs(cfg,quote,pip):
    if cfg.get("Profile")=="BTC_24X7_V1":
        return quote*cfg["SlippageBps"]/10000,quote*cfg["CostReserveBps"]/10000
    return cfg["SlippagePips"]*pip,cfg["CostReservePips"]*pip


def replay(es,js,tape,cfg,begin,end,policy,costs=1.0,threshold=.6,independent=False):
    """Both policies use a common decision TTL latency. Jev must arrive before it.
    Independent mode estimates all-candidate labels, never a tradable portfolio.
    Size is continuous normalized R: broker volume rounding/conversion is excluded.
    """
    equity=peak=day_equity=1.0; day=None; day_count=0
    daily_latch=global_latch=False
    last_entry=busy_until=dt.datetime.min.replace(tzinfo=UTC)
    trades=[]; excluded=defaultdict(int)
    for e in es:
        s,c=e["Data"]["Snapshot"],e["Data"]["Candidate"]
        decision=stamp(s["DecisionUtc"])
        if not begin<=decision<end: continue
        ready=decision+dt.timedelta(seconds=cfg["DecisionTtlSeconds"])
        if not independent and ready<busy_until: continue
        if ready.date()!=day:
            day,day_equity,day_count,daily_latch=ready.date(),equity,0,False
        if c["Direction"]=="NONE" or EXOGENOUS.intersection(e["Data"]["HardReasons"]): continue
        jr=js.get(s["Id"]); j=jr["Data"] if jr else {}
        available=jr is not None and jr["_time"]<=ready and j.get("Error") is None
        approved=available and j.get("Direction")==c["Direction"] and j.get("Confidence",0)>=threshold
        if policy=="jev" and not approved: continue
        if not independent:
            if global_latch or daily_latch or ready<busy_until or day_count>=cfg["MaxTradesPerDay"]: continue
            if (ready-last_entry).total_seconds()<cfg["CooldownMinutes"]*60: continue
        ix=tape.after(ready,2)
        if ix is None:
            excluded["NO_ENTRY_QUOTE"]+=1; continue
        sign=1 if c["Direction"]=="LONG" else -1
        bid,ask=tape.q[ix]; pip=s["PipSize"]; atr=s["M15"]["Atr"]
        max_spread=(bid+ask)/2*cfg["MaxSpreadBps"]/10000 if cfg.get("Profile")=="BTC_24X7_V1" else cfg["MaxSpreadPips"]*pip
        if ask-bid>max_spread or ask-bid>cfg["MaxSpreadAtr"]*atr: continue
        if abs((bid+ask-s["Bid"]-s["Ask"])/2)>cfg["MaxEntryDriftAtr"]*atr: continue
        quote=ask if sign==1 else bid
        base_slip,reserve=price_costs(cfg,quote,pip); slip=base_slip*costs
        entry=quote+sign*slip
        distance=sign*(quote-c["StopAnchor"])+base_slip
        if not cfg["MinStopAtr"]*atr<=distance<=cfg["MaxStopAtr"]*atr: continue
        if c["Obstacle"] and sign*(c["Obstacle"]-quote)<cfg["TargetR"]*distance+base_slip+.1*atr: continue
        deadline=tape.t[ix]+dt.timedelta(minutes=cfg["HoldMinutes"])
        if deadline>=end:
            excluded["FOLD_BOUNDARY"]+=1; continue
        last=tape.after(deadline)
        if last is None or not tape.covered(ix,last):
            excluded["CENSORED_PATH"]+=1; continue
        denom=distance+reserve; fee=reserve*costs
        risk_cash=(1 if independent else equity)*cfg["RiskFraction"]
        mfe=mae=0.0; reason="TIME_EXIT"; entry_day=day; day_count+=1
        for k in range(ix,last+1):
            b,a=tape.q[k]; exit_quote=b if sign==1 else a
            change=sign*(exit_quote-entry); mfe,mae=max(mfe,change),min(mae,change)
            if not independent:
                marked=equity+risk_cash*(change-fee)/denom
                if tape.t[k].date()!=day:
                    day=tape.t[k].date();day_equity=marked;day_count=0;daily_latch=False
                peak=max(peak,marked)
                daily_latch|=marked<=day_equity*(1-cfg["DailyLossFraction"])
                global_latch|=marked<=peak*(1-cfg["MaxDrawdownFraction"])
            if change<=-distance: reason="STOP"; break
            if change>=cfg["TargetR"]*distance: reason="TARGET"; break
            if not independent and (daily_latch or global_latch): reason="RISK_KILL"; break
        # Bid/ask already pays spread. Do not subtract spread twice.
        gross=sign*(exit_quote-sign*slip-entry); net_r=(gross-fee)/denom
        if not independent:
            equity+=risk_cash*net_r; peak=max(peak,equity)
            daily_latch|=equity<=day_equity*(1-cfg["DailyLossFraction"])
            global_latch|=equity<=peak*(1-cfg["MaxDrawdownFraction"])
        last_entry,busy_until=tape.t[ix],tape.t[k]
        answer=((j.get("Setup") or {}).get("Reply") or {}).get("answers",{}).get("direction",{})
        trades.append(dict(id=s["Id"],day=busy_until.date().isoformat(),entry_day=entry_day.isoformat(),policy=policy,entry_utc=last_entry.isoformat(),
            exit_utc=busy_until.isoformat(),direction=c["Direction"],net_r=net_r,gross_r=gross/denom,
            fee_r=fee/denom,reason=reason,mfe_r=mfe/denom,mae_r=mae/denom,
            confidence=answer.get("confidence"),raw_direction=answer.get("choice"),
            jev_approved=approved,equity=equity,pnl=risk_cash*net_r))
    return trades,dict(excluded)


def summary(ts):
    return dict(n=len(ts),mean_net_r=sum(t["net_r"] for t in ts)/len(ts) if ts else None,
                pnl=sum(t["pnl"] for t in ts),win_rate=sum(t["net_r"]>0 for t in ts)/len(ts) if ts else None)


def bootstrap(a,b,days,seed=731,n=2000,block=5):
    left,right=defaultdict(float),defaultdict(float)
    for t in a: left[t["day"]]+=t["pnl"]
    for t in b: right[t["day"]]+=t["pnl"]
    values=[left[d]-right[d] for d in days]
    if len(values)<20: return None
    rng=random.Random(seed); estimates=[]
    for _ in range(n):
        sample=[]
        while len(sample)<len(values):
            start=rng.randrange(len(values)); sample.extend(values[(start+j)%len(values)] for j in range(block))
        estimates.append(sum(sample[:len(values)])/len(values))
    estimates.sort()
    return dict(mean_daily_increment=sum(values)/len(values),lower95=estimates[int(.025*n)],
                upper95=estimates[int(.975*n)],block_days=block,days=len(days))


def walk_forward(es,js,tape,cfg,manifest):
    folds=[]; previous_end=None
    for f in manifest["folds"]:
        train_end,start,end=map(stamp,(f["train_end"],f["test_start"],f["test_end"]))
        if start-train_end<dt.timedelta(minutes=90) or end<=start or (previous_end and start<previous_end):
            raise ValueError("Overlapping folds or invalid >=90 minute embargo")
        previous_end=end
        b,xb=replay(es,js,tape,cfg,start,end,"baseline")
        j,xj=replay(es,js,tape,cfg,start,end,"jev")
        labels,xl=replay(es,js,tape,cfg,start,end,"baseline",independent=True)
        stress,_=replay(es,js,tape,cfg,start,end,"jev",costs=2)
        days=sorted({r["_time"].date().isoformat() for r in es if start<=r["_time"]<end})
        bins=[]
        for lo,hi in ((0,.4),(.4,.6),(.6,.7),(.7,.8),(.8,.9),(.9,1.000001)):
            group=[t for t in labels if t["raw_direction"]==t["direction"] and t["confidence"] is not None and lo<=t["confidence"]<hi]
            bins.append(dict(interval=[lo,min(hi,1)],**summary(group)))
        folds.append(dict(id=f["id"],baseline=summary(b),jev=summary(j),
            incremental_daily_ci=bootstrap(j,b,days),confidence_bins=bins,jev_double_costs=summary(stress),
            censored_baseline=xb,censored_jev=xj,censored_labels=xl,baseline_trades=b,jev_trades=j,
            independent_candidate_labels=labels))
    return dict(method="Frozen .60 threshold chronological OOS evaluation; no fitting",folds=folds)


def main():
    p=argparse.ArgumentParser(); p.add_argument("journal",type=Path);p.add_argument("--out",type=Path,required=True)
    p.add_argument("--folds",type=Path);a=p.parse_args();a.out.mkdir(parents=True,exist_ok=True)
    rows=load(a.journal);ingest(rows,a.out/"audit.sqlite");es,js,cfg=index(rows);tape=Tape(rows)
    (a.out/"markouts.json").write_text(json.dumps(markouts(es,js,tape),allow_nan=False),encoding="utf-8")
    if a.folds:
        result=walk_forward(es,js,tape,cfg,json.loads(a.folds.read_text(encoding="utf-8")))
        (a.out/"walk_forward.json").write_text(json.dumps(result,indent=2,allow_nan=False),encoding="utf-8")


if __name__=="__main__": main()
