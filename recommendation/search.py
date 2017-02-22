from config import MAX_SCORE, MIN_SCORE
from database import invoke_user_db
import numpy as np
import math
from nltk.corpus import wordnet as wn
from nltk.tag import pos_tag
from sklearn.feature_extraction.text import CountVectorizer
from recommendation import WeightedSlope, KNearestUsers, BipolarSlope

a = "Two Jedi Knights escape a hostile blockade to find allies and come across a young boy who may " \
    "bring balance to the Force, but the long dormant Sith resurface to reclaim their old glory."
b = "Ten years after initially meeting, Anakin Skywalker shares a forbidden romance with Padmé, while Obi-Wan " \
    "investigates an assassination attempt on the Senator and discovers a secret clone army crafted for the Jedi."

print(a)

words = set(a.replace(',', '').replace('.', '').split(' '))
nouns = set(word for word, pos in pos_tag(words) if pos in {'NNP', 'NN'})
print(nouns)

print(b)

words2 = set(b.replace(',', '').replace('.', '').split(' '))
nouns2 = set(word for word, pos in pos_tag(words2) if pos in {'NNP', 'NN'})
print(nouns2)

vectorizer = CountVectorizer(min_df=0.55)
vectorizer.fit_transform([' '.join(nouns), ' '.join(nouns2)])
features = vectorizer.get_feature_names()
print(features)

sims = []
for noun in nouns:
    for noun2 in nouns2:
        if noun == noun2:
            print(noun, noun2, 1.)
            sims.append(1.)
        else:
            wordFromList1 = wn.synsets(noun)
            wordFromList2 = wn.synsets(noun2)
            if wordFromList1 and wordFromList2:
                s = wordFromList1[0].wup_similarity(wordFromList2[0])
                print(noun, noun2, s)
                sims.append(s)
print(sims)


def get_approx_correlation(x: np.array, y: np.array) -> float:
    if len(x) != len(y) or len(x.shape) != 1 or len(y.shape) != 1:
        return None
    n = len(x)
    sum_x = sum(x)
    sum_y = sum(y)
    return (np.dot(x, y) - sum_x * sum_y / n) / math.sqrt((np.dot(x, x) - sum_x * sum_x / n) *
                                                          (np.dot(y, y) - sum_y * sum_y / n))


def get_cosine_similarity(x: np.array, y: np.array) -> float:
    if len(x) != len(y) or len(x.shape) != 1 or len(y.shape) != 1:
        return None
    return np.dot(x, y) / (math.sqrt(np.dot(x, x) * np.dot(y, y)))


def get_average(data: np.array) -> float:
    ans = 0.
    for d in data:
        ans += d
    return ans / len(data)


def get_median(data: np.array) -> float:
    data.sort()
    if len(data) % 2 == 1:
        return data[int(len(data) / 2) + 1]
    else:
        return (data[int(len(data) / 2)] + data[int(len(data) / 2) + 1]) / 2


def get_absolute_standard_deviation(data: np.array, median: float = None) -> float:
    if not median:
        median = get_median(data)
    data = abs(data - median)
    return sum(data) / len(data)


def standardize_scores(ratings: dict) -> dict:
    """We should use standardizing when attributes has different scales

    :param ratings:
    :return:
    """
    scores = np.array(list(ratings.values()))
    median = get_median(scores)
    asd = get_absolute_standard_deviation(scores, median)
    return {key: (score - median) / asd for key, score in ratings.items()}


def normalize_scores(ratings: dict) -> dict:
    return {key: (2 * score - (MAX_SCORE + MIN_SCORE)) / (MAX_SCORE - MIN_SCORE) for key, score in ratings.items()}


def get_nearest_user(user_id: int, database: dict) -> int:
    user_ids = set(database.keys())
    if user_id in user_ids:
        normalized = normalize_scores(database[user_id])
        user_films = set(database[user_id])
        user_ids.remove(user_id)
        for uid in user_ids:
            common_films = set(database[uid])
            common_films = user_films.intersection(common_films)
            if common_films:
                for film in common_films:
                    database[user_id][film]
        return common_films

    else:
        return None


if __name__ == '__main__':
    user_db = invoke_user_db()
    print(get_nearest_user(1, user_db))


def normalize_scores(ratings: dict, min_score: float = MIN_SCORE, max_score: float = MAX_SCORE) -> dict:
    if max_score > min_score:
        return {key: 5 * (score - min_score) / (max_score - min_score) for key, score in ratings.items()}
    else:
        return ratings

# (2 * score - (max_score + min_score)) / (max_score - min_score)

# if __name__ == '__main__':
#     user_db = invoke_user_db()
#     user_db = {uid: normalize_scores(user_db[uid], min(user_db[uid].values()), max(user_db[uid].values()))
#                for uid in user_db}
#     # print(nearest)
#     print(KNearestUsers.get_recommendation(1, user_db, 10, KNearestUsers.COSINE_SIMILARITY))
#
#     nearest = KNearestUsers.get_nearest_users(1, user_db, 10, KNearestUsers.COSINE_SIMILARITY)
#     nearest.append((1, 1.))
#     unrated_films = {x for user in nearest if user for x in user_db[user[0]]}  # - set(user_db[1])
#     rates = {user[0]: {film: user_db[user[0]][film] for film in unrated_films if film in user_db[user[0]]}
#              for user in nearest}
#     # print(rates)
#     print(WeightedSlope.get_recommendation(1, rates))
#     print(BipolarSlope.get_recommendation(1, rates))
# ratings = dict(Amy={"Taylor Swift": 4, 'PSY': 3, 'Whitney Houston': 4}, Ben={"Taylor Swift": 5, 'PSY': 2},
#                Clara={'PSY': 3.5, "Whitney Houston": 4}, Daisy={"Taylor Swift": 5, "Whitney Houston": 3})
