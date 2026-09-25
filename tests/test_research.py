import datetime as dt
import unittest
from research import Tape, stamp, markouts, replay, bootstrap, price_costs


def quote(at,bid,ask):
    return {"Kind":"QUOTE","_time":at,"Data":{"Bid":bid,"Ask":ask}}


class ResearchTests(unittest.TestCase):
    def test_btc_costs_do_not_depend_on_broker_pip_convention(self):
        cfg={"Profile":"BTC_24X7_V1","SlippageBps":1,"CostReserveBps":10}
        self.assertEqual(price_costs(cfg,100000,.01),(10,100))
        self.assertEqual(price_costs(cfg,100000,1),(10,100))

    def test_no_future_quote_backfill(self):
        t=stamp("2026-01-01T10:00:00Z")
        tape=Tape([quote(t,100,101),quote(t+dt.timedelta(seconds=45),102,103)])
        self.assertIsNone(tape.after(t+dt.timedelta(seconds=1),2))
        self.assertFalse(tape.covered(0,1))

    def test_same_price_round_trip_pays_spread(self):
        t=stamp("2026-01-01T10:00:00Z")
        tape=Tape([quote(t+dt.timedelta(seconds=k),100,101) for k in range(0,5410,10)])
        snapshot={"Id":"a","ObservedUtc":t.isoformat(),"Bid":100,"Ask":101}
        labels=markouts([{"Data":{"Snapshot":snapshot}}],{},tape)
        self.assertTrue(all(x["valid"] for x in labels))
        self.assertTrue(all(x["price_return"]==-1 for x in labels))
        self.assertTrue(all(x["mae"]==-1 and x["mfe"]==0 for x in labels))

    def test_missing_horizon_is_not_zero(self):
        t=stamp("2026-01-01T10:00:00Z")
        s={"Id":"a","ObservedUtc":t.isoformat(),"Bid":100,"Ask":101}
        labels=markouts([{"Data":{"Snapshot":s}}],{},Tape([quote(t,100,101)]))
        self.assertTrue(all(not x["valid"] for x in labels))
        self.assertTrue(all("price_return" not in x for x in labels))

    def test_gap_stop_uses_observed_quote(self):
        t=stamp("2026-01-01T10:00:00Z")
        cfg=dict(DecisionTtlSeconds=20,MaxTradesPerDay=4,CooldownMinutes=75,
                 MaxSpreadPips=2,MaxSpreadAtr=1,MaxEntryDriftAtr=1,SlippagePips=0,
                 MinStopAtr=.1,MaxStopAtr=5,TargetR=1.5,HoldMinutes=60,
                 CostReservePips=.1,RiskFraction=.001,DailyLossFraction=.01,MaxDrawdownFraction=.1)
        s=dict(Id="a",DecisionUtc=t.isoformat(),Bid=100,Ask=101,PipSize=1,M15={"Atr":2})
        c=dict(Direction="LONG",StopAnchor=99,Obstacle=0)
        e={"_time":t,"Data":{"Snapshot":s,"Candidate":c,"HardReasons":[]}}
        quotes=[quote(t+dt.timedelta(seconds=k),100 if k<30 else 95,101 if k<30 else 96)
                for k in range(0,3650,10)]
        trades,_=replay([e],{},Tape(quotes),cfg,t,t+dt.timedelta(hours=2),"baseline")
        self.assertEqual(trades[0]["reason"],"STOP")
        self.assertLess(trades[0]["net_r"],-2)  # not magically filled at -1R

    def test_paired_identical_portfolios_have_zero_increment(self):
        days=[f"2026-01-{i:02d}" for i in range(1,31)]
        trades=[{"day":d,"pnl":.001} for d in days]
        result=bootstrap(trades,trades,days,n=100)
        self.assertEqual(result["lower95"],0)
        self.assertEqual(result["upper95"],0)


if __name__=="__main__": unittest.main()
