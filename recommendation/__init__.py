import math
import numpy as np
from config import GENRES, MAX_SCORE, MIN_SCORE, SEP


class UserRatings(object):
    def __init__(self, uid: int, ratings: dict):
        self.uid = uid
        self.ratings = ratings

    def __repr__(self):
        return str(self.uid + " with " + len(self.ratings) + " rated movies")


class Film(object):
    def __init__(self, fid: int, year: int, genres: set, tags: set, ratings: dict):
        self.fid = fid
        self.year = year
        self.genres = genres
        self.tags = tags
        self.ratings = ratings

    def __repr__(self):
        return str(self.fid + " with " + len(self.ratings) + " rated movies")
        return str(self.uid) + " with " + str(len(self.ratings)) + " rated movies"


class Film(object):
    def __init__(self, fid: int, title: str, year: int, genres: set, directors: set, writers: set, actors: set,
                 country: set, language: set, runtime: str, plot: str, ratings: dict):
        self.fid = fid
        self.title = title
        self.year = year
        self.genres = genres.intersection(GENRES)
        self.directors = directors
        self.writers = writers
        self.actors = actors
        self.country = country
        self.language = language
        self.runtime = runtime
        self.plot = plot
        self.ratings = ratings

    def __eq__(self, other):
        return self.fid == other.fid and self.year == other.year and self.directors == other.directors

    def __hash__(self):
        return self.fid

    def __repr__(self):
        return self.title + SEP + self.year + SEP + ''.join(self.genres) + SEP + ''.join(self.directors) + SEP + \
               ''.join(self.writers) + SEP + ''.join(self.actors) + SEP + ''.join(self.country) + SEP + \
               ''.join(self.language) + SEP + self.runtime + SEP + self.plot


class BipolarSlope(object):
    @staticmethod
    def deviation(product1: str, product2: str, ranks: dict, like: bool) -> float:
        num = 0
        ans = 0.
        for rank in ranks.values():
            aver_rank = sum(rank.values()) / len(rank)
            if product1 in rank and product2 in rank and (
                                    rank[product1] > aver_rank and rank[product2] > aver_rank and like or
                                    rank[product1] < aver_rank and rank[product2] < aver_rank and not like):
                num += 1
                ans += rank[product1] - rank[product2]
        if num == 0:
            return num, ans
        return num, ans / num

    @staticmethod
    def predicted_rank(user: str, product: str, ranks: dict) -> float:
        total_num = 0
        ans = 0.
        user_products = ranks[user]
        aver_rank = sum(user_products.values()) / len(user_products)
        for user_product in user_products:
            if user_product != product:
                if user_products[user_product] > aver_rank:
                    num, dev = BipolarSlope.deviation(product, user_product, ranks, True)
                else:
                    num, dev = BipolarSlope.deviation(product, user_product, ranks, False)
                total_num += num
                ans += (dev + user_products[user_product]) * num
        if total_num == 0:
            return -(MAX_SCORE ** 2 + MIN_SCORE ** 2), 0
        else:
            return ans / total_num, total_num

    @staticmethod
    def get_recommendation(user, ranks: dict) -> dict:
        products = set(y for x in ranks.values() for y in x.keys())
        ans = set()
        for product in products:
            if product not in ranks[user]:
                ans.add((product, BipolarSlope.predicted_rank(user, product, ranks)))
        ans = sorted(ans, key=lambda x: (round(x[1][0], 4), x[1][1], x[0]), reverse=True)
        return ans


class WeightedSlope(object):
    @staticmethod
    def deviation(product1: str, product2: str, ranks: dict) -> float:
        num = 0
        ans = 0.
        for rank in ranks.values():
            if product1 in rank and product2 in rank:
                num += 1
                ans += rank[product1] - rank[product2]
        if num == 0:
            return num, ans
        return num, ans / num

    @staticmethod
    def predicted_rank(user: str, product: str, ranks: dict) -> float:
        total_num = 0
        ans = 0.
        user_products = ranks[user]
        for user_product in user_products:
            if user_product != product:
                num, dev = WeightedSlope.deviation(product, user_product, ranks)
                total_num += num
                ans += (dev + user_products[user_product]) * num
        if total_num == 0:
            return -(MAX_SCORE ** 2 + MIN_SCORE ** 2), 0
        else:
            return ans / total_num, total_num

    @staticmethod
    def get_recommendation(user, ranks: dict) -> dict:
        oth_users = set(ranks.keys())
        oth_users.remove(user)
        unrated_products = {y for x in oth_users for y in ranks[x].keys()}
        ans = set()
        for product in unrated_products:
            ans.add((product, WeightedSlope.predicted_rank(user, product, ranks)))
        ans = sorted(ans, key=lambda x: (round(x[1][0], 4), x[1][1], x[0]), reverse=True)
        return ans


class KNearestUsers(object):
    COSINE_SIMILARITY = -10
    PEARSON_SIMILARITY = 10

    @staticmethod
    def get_nearest_users(user_id: int, database: dict, k: int, method: int) -> int:
        user_ids = set(database.keys())
        if user_id in user_ids:
            res = [(0, -1.) for _ in range(k)]
            user_films = set(database[user_id])
            user_ids.remove(user_id)
            for uid in user_ids:
                oth_user_films = set(database[uid])
                common_films = user_films.intersection(oth_user_films)
                if len(common_films) > 10:
                    user_ratings = np.array(list(database[user_id].values()))
                    oth_user_ratings = np.array(list(database[uid].values()))
                    scalar_prod = 0.
                    for film in common_films:
                        scalar_prod += database[user_id][film] * database[uid][film]
                    user_ratings_norm = user_ratings.dot(user_ratings)
                    oth_user_ratings_norm = oth_user_ratings.dot(oth_user_ratings)
                    if method == KNearestUsers.COSINE_SIMILARITY:
                        user_ratings_norm = math.sqrt(user_ratings_norm)
                        oth_user_ratings_norm = math.sqrt(oth_user_ratings_norm)
                        ans = scalar_prod / (user_ratings_norm * oth_user_ratings_norm)
                    elif method == KNearestUsers.PEARSON_SIMILARITY:
                        user_ratings_sum = sum(user_ratings)
                        oth_user_ratings_sum = sum(oth_user_ratings)
                        num = len(user_films.union(oth_user_films))
                        ans = (scalar_prod - user_ratings_sum * oth_user_ratings_sum / num) / math.sqrt(
                            (user_ratings_norm - user_ratings_sum ** 2 / num) *
                            (oth_user_ratings_norm - oth_user_ratings_sum ** 2 / num)
                        )
                    if ans > res[0][1]:
                        for ind in range(1, k):
                            if ans < res[ind][1]:
                                del res[0]
                                res.insert(ind - 1, (uid, ans))
                                break
                        if ans > res[ind][1]:
                            del res[0]
                            res.insert(ind, (uid, ans))
            if res:
                res.sort(key=lambda x: (round(x[1], 4), x[0]), reverse=True)
            return res
        else:
            return None

    @staticmethod
    def get_recommendation(user_id: int, database: dict, k: int, method: int):
        nearest_users = KNearestUsers.get_nearest_users(user_id, database, k, method)
        unrated_films = {x for user in nearest_users if user for x in database[user[0]]} - set(database[1])
        res = []
        max_users = -1
        for film in unrated_films:
            users = {user for user in nearest_users if film in database[user[0]]}
            if len(users) > max_users:
                max_users = len(users)
            sum_sim = sum({sim for _, sim in users})
            aver_rating = 0.
            for user in users:
                aver_rating += database[user[0]][film] * user[1] / sum_sim
            res.append(({film: aver_rating}, len(users), sum_sim / len(users)))
        res.sort(key=lambda x: (x[1], list(x[0].values())[0], x[2]), reverse=True)
        return res
