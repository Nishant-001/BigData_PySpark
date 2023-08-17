from collections import deque

numbers = [1, 2, 3, 4, 5, 6]
# print(numbers)
scores = {"Alice": 85, "Bob": 92, "charlie": 83}
scores["Alice"] = 43
scores["peter"] = 100
# print(scores)

scores1 = {"Alice": 85, "stewie": 92, "margret": 83}
scores2 = {"ram": 100, "alice": 20}

merged_scores = {**scores, **scores1, **scores2}
print(len(merged_scores))
scores.update(scores1)
print(merged_scores)

# print(scores)
# for key in scores:
#     print(key)

# for key,value in scores.items():
#     print(key, "-", value)
# liked_list = deque()
# liked_list.append((1, 2, 3, 3))
#
#
# stack = []
# stack.append(("alice", "bob"))
# stack.pop()
