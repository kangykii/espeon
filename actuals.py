"""Reconstruct actual fill-to-exit excursions and fill costs from the journal.
Usage: python actuals.py runtime/events.jsonl --out actual_trades.json
Not executed by author. Missing broker history remains explicitly unreconciled.
"""
import argparse
import bisect
import json
from pathlib import Path
from research import load, Tape, stamp


def report(rows):
    tape=Tape(rows); fills={}; intents={}; exits=[]
    for row in rows:
        d=row["Data"]
        if row["Kind"]=="FILL": fills[d["Position"]["Id"]]=d
        elif row["Kind"]=="EXIT_INTENT": intents[d["Id"]]=d
        elif row["Kind"]=="EXIT":
            p=d["Position"]; fill=fills.get(p["Id"]); deals=d["Deals"]
            out=dict(evaluation_id=d["Id"],position_id=p["Id"],broker_reason=d["BrokerReason"],
                     requested_reason=d["RequestedReason"],gross=p["GrossProfit"],net=p["NetProfit"],
                     commissions=p["Commissions"],swap=p["Swap"],history_pending=not bool(deals),
                     entry_spread=None if fill is None else fill["Ask"]-fill["Bid"],
                     entry_slippage_pips=None if fill is None else fill["SlippagePips"],
                     stop=p["StopLoss"],target=p["TakeProfit"],mfe=None,mae=None,path_valid=False,deals=deals)
            if deals:
                start=stamp(p["EntryTime"]); end=max(stamp(t["ClosingTime"]) for t in deals)
                first=tape.after(start); last=bisect.bisect_right(tape.t,end)-1
                valid=first is not None and last>=first and (end-tape.t[last]).total_seconds()<=30 and tape.covered(first,last)
                sign=1 if p["Direction"]=="Buy" else -1
                if valid:
                    changes=[sign*((b if sign==1 else a)-p["EntryPrice"]) for b,a in tape.q[first:last+1]]
                    # Include actual closing fills, including a gap beyond the final sampled quote.
                    changes.extend(sign*(x["ClosingPrice"]-p["EntryPrice"]) for x in deals)
                    out.update(mfe=max(0,max(changes)),mae=min(0,min(changes)),path_valid=True)
                request=intents.get(p["Id"])
                reference=(request["Bid"] if sign==1 else request["Ask"]) if request else None
                if reference is None and d["BrokerReason"]=="StopLoss": reference=p["StopLoss"]
                if reference is None and d["BrokerReason"]=="TakeProfit": reference=p["TakeProfit"]
                volume=sum(x["VolumeInUnits"] for x in deals)
                vwap=sum(x["ClosingPrice"]*x["VolumeInUnits"] for x in deals)/volume
                out.update(exit_vwap=vwap,exit_reference=reference,
                           adverse_exit_slippage_price=None if reference is None else sign*(reference-vwap),
                           hold_minutes=(end-start).total_seconds()/60,
                           gross_deals=sum(x["GrossProfit"] for x in deals),net_deals=sum(x["NetProfit"] for x in deals))
            exits.append(out)
    return exits


if __name__=="__main__":
    p=argparse.ArgumentParser();p.add_argument("journal",type=Path);p.add_argument("--out",type=Path,required=True)
    a=p.parse_args();a.out.write_text(json.dumps(report(load(a.journal)),indent=2,allow_nan=False),encoding="utf-8")
