BUILD_PRIORITY = ["jack", "damager", "runner", "ninja", "tank"]

TRAIT_SLOTS = [
    "beak",
    "comb",
    "eyes",
    "feet",
    "wings",
    "tail",
    "body",
]

BUILD_RULES = {
    "damager": {
        "label": "Damager",
        "slots": {
            "beak": ["Wormtongue", "Raven", "Ironclad", "Piercing Fang"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Mahiwaga", "Luntian", "Makopa", "Sibat"],
            "wings": ["Slenduh", "Awra"],
            "tail": ["Abaniko", "Onagadori"],
            "body": ["Jordi"],
        },
    },
    "tank": {
        "label": "Tank",
        "slots": {
            "beak": ["Radiant", "Flare", "Ashfire", "Boneblade"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": [],
            "feet": ["Onyx", "Chernobyl", "Paleclaws", "Catriona"],
            "wings": ["Slenduh", "Awra"],
            "tail": [],
            "body": ["Hoeltaf"],
        },
    },
    "runner": {
        "label": "Runner",
        "slots": {
            "beak": ["Haki", "Nightwave", "Touca", "Blade Spire"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Mahiwaga", "Luntian", "Makopa", "Sibat"],
            "wings": ["Slenduh", "Awra"],
            "tail": ["Abaniko", "Onagadori"],
            "body": ["Jordi"],
        },
    },
    "ninja": {
        "label": "Ninja",
        "slots": {
            "beak": ["Wormtongue", "Raven", "Ironclad", "Piercing Fang"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Onyx", "Chernobyl", "Paleclaws", "Catriona"],
            "wings": ["Slenduh", "Awra"],
            "tail": ["Abaniko", "Onagadori", "Starjeatl", "Carota", "Agave", "Rengoku"],
            "body": ["Wickid", "Jordi"],
        },
    },
    "jack": {
        "label": "Jack",
        "slots": {
            "beak": ["Aurora", "Verdant", "Greenbill", "Bluelip"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Hepa Lane", "Kaliskis", "Mewling Tiger", "Dionela"],
            "wings": ["Slenduh", "Awra"],
            "tail": ["Abaniko", "Onagadori"],
            "body": ["Emilia"],
        },
    },
}
