from datetime import datetime
from dotamatch.api import Api


class MatchDetails(Api):
    url = "https://api.steampowered.com/IDOTA2Match_570/GetMatchDetails/V001/?"

    def __init__(self, key):
        super(MatchDetails, self).__init__(key)

    def match(self, match_id):
        return Match(self, **self._get(match_id=match_id)['result'])


class Match(object):
    def __init__(self, parent, **kwargs):
        self.parent = parent
        self.players = []

        for key, value in kwargs.items():
            setattr(self, key, value)

        if hasattr(self, 'start_time'):
            self.start_time = datetime.utcfromtimestamp(self.start_time)
        else:
            self.start_time = datetime.utcnow()

    def player(self, account_id):
        for player in self.players:
            if player['account_id'] == account_id:
                return player
