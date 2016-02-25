import pprint, os
import json, jsonpickle
from dotamatch import get_key
from dotamatch.history import MatchHistoryBySequenceNum
from dotamatch.players import PlayerSummaries
from dotamatch.heroes import Heroes
import schedule, time
from pymongo import MongoClient, ASCENDING

class Main():
    def __init__(self):
        self.client = MongoClient(os.environ['MONGO_URL'])
        self.db = self.client.dota2stats
        self.matches = self.db.matches
        self.analytics = self.db.analytics

    def startIntepret(self):
        data = self.matches.find({'interpret_version': 0})
        radiant_wins = 0
        dire_wins = 0
        first_match = None
        latest_match = None
        hero_data = {}
        for i in range(112):
            hero_data[i] = {}
            hero_data[i]["total_match"] = 0
            hero_data[i]["kills"] = 0
            hero_data[i]["deaths"] = 0
            hero_data[i]["assists"] = 0
            hero_data[i]["xp_per_min"] = 0
            hero_data[i]["gold_per_min"] = 0
            hero_data[i]["wins"] = 0
            hero_data[i]["loses"] = 0

        for d in data:
            if d["game_mode"] in [1, 2, 3, 4, 5, 22]:
                if first_match == None:
                    first_match = d["start_time"]

                if d["radiant_win"]:
                    radiant_wins += 1
                else:
                    dire_wins += 1

                # loop hero to get hero data
                for p in d["players"]:
                    hero_data[p["hero_id"]]["total_match"] += 1
                    hero_data[p["hero_id"]]["kills"] += p["kills"]
                    hero_data[p["hero_id"]]["deaths"] += p["deaths"]
                    hero_data[p["hero_id"]]["assists"] += p["assists"]
                    hero_data[p["hero_id"]]["xp_per_min"] += p["xp_per_min"]
                    hero_data[p["hero_id"]]["gold_per_min"] += p["gold_per_min"]

                    if p < 10 and d["radiant_win"]:
                        hero_data[p["hero_id"]]["wins"] += 1
                    else:
                        hero_data[p["hero_id"]]["loses"] += 1

                    if p > 10 and d["radiant_win"]:
                        hero_data[p["hero_id"]]["loses"] += 1
                    else:
                        hero_data[p["hero_id"]]["wins"] += 1

                latest_match = d["start_time"]

            self.matches.update_one({"_id": d["_id"]}, {"$set": {"interpret_version": 1}})

        data = self.analytics.find_one({"type": "overall"})
        if(data):
            self.analytics.update_one({"type": "overall"}, {"$set": {"radiant_wins": data["radiant_wins"] + radiant_wins,
                                                               "dire_wins": data["dire_wins"] + dire_wins,
                                                                "latest_match": latest_match,
                                                               "total_matches": data["total_matches"] + radiant_wins + dire_wins}})
        else:
            self.analytics.insert_one({"type": "overall", "first_match": first_match,
                                    "latest_match": latest_match,
                                  "radiant_wins": radiant_wins, "dire_wins": dire_wins,
                                  "total_matches": radiant_wins + dire_wins})

        for key, value in hero_data.iteritems():
            if(value["total_match"] < 1):
                continue

            data = self.analytics.find_one({"type": "hero", "hero_id": key})
            if(data):

                avg_assists = float(value["assists"]) / float(value["total_match"])
                avg_deaths = float(value["deaths"]) / float(value["total_match"])
                avg_kills = float(value["kills"]) / float(value["total_match"])
                avg_gold_per_min = float(value["gold_per_min"]) / float(value["total_match"])
                avg_xp_per_min = float(value["xp_per_min"]) / float(value["total_match"])
                avg_assists = (data["avg_assists"] + avg_assists) / 2.0
                avg_deaths = (data["avg_deaths"] + avg_deaths) / 2.0
                avg_kills = (data["avg_kills"] + avg_kills) / 2.0
                try:
                    kd_ratio = avg_kills / avg_deaths
                except:
                    kd_ratio = avg_kills
                avg_gold_per_min = (data["avg_gold_per_min"] + avg_gold_per_min) / 2.0
                avg_xp_per_min = (data["avg_xp_per_min"] + avg_xp_per_min) / 2.0

                twins = data["wins"] + value["wins"];
                wl_ratio = (float(data["wins"]) + float(value["wins"])) / (float(data["loses"]) + float(value["loses"]))

                self.analytics.update_one({"type": "hero", "hero_id": key}, {"$set": {
                                      "avg_assists": avg_assists,
                                      "avg_deaths": avg_deaths,
                                      "avg_gold_per_min": avg_gold_per_min,
                                      "avg_kills": avg_kills,
                                      "loses": data["loses"] + value["loses"],
                                      "wins": twins,
                                      "kd_ratio": kd_ratio,
                                      "wl_ratio": wl_ratio,
                                      "avg_xp_per_min": avg_xp_per_min,
                                      "total_match": data["total_match"] + value["total_match"]}})

            else:
                avg_assists = float(value["assists"]) / float(value["total_match"])
                avg_deaths = float(value["deaths"]) / float(value["total_match"])
                avg_kills = float(value["kills"]) / float(value["total_match"])
                try:
                    kd_ratio = avg_kills / avg_deaths
                except:
                    kd_ratio = avg_kills
                avg_gold_per_min = float(value["gold_per_min"]) / float(value["total_match"])
                avg_xp_per_min = float(value["xp_per_min"]) / float(value["total_match"])

                self.analytics.insert_one({"type": "hero",
                                      "hero_id": key,
                                      "avg_assists": avg_assists,
                                      "avg_deaths": avg_deaths,
                                      "avg_gold_per_min": avg_gold_per_min,
                                      "avg_kills": avg_kills,
                                      "loses": value["loses"],
                                      "wins": value["wins"],
                                      "kd_ratio": kd_ratio,
                                      "wl_ratio": float(value["wins"]) / float(value["loses"]),
                                      "avg_xp_per_min": avg_xp_per_min,
                                      "total_match": value["total_match"]})



print "Starting the analytics bot to interpet the data"
m = Main()
schedule.every(1).minutes.do(m.startIntepret)

while True:
    schedule.run_pending()
    time.sleep(1)
