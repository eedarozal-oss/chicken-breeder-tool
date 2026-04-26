BUILD_PRIORITY = ["levi", "shanks", "killua", "hybrid 2", "hybrid 1"]

BUILD_INSTINCT_TIERS = {
    "killua": ["aggressive", "swift", "reckless", "elusive", "relentless", "blazing"],
    "shanks": ["steadfast", "stalwart", "resolute", "tenacious", "bulwark", "enduring"],
    "levi": ["balanced", "unyielding", "vicious", "adaptive", "versatile"],
}

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
    "killua": {
        "label": "Killua",
        "slots": {
            "beak": [
                "Haki",
                "Nightwave",
                "Touca",
                "Blade Spire",
                "Wormtongue",
                "Raven",
                "Ironclad",
                "Piercing Fang",
            ],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Mahiwaga", "Luntian", "Makopa", "Sibat"],
            "wings": ["Slenduh", "Awra"],
            "tail": ["Starjeatl", "Carota"],
            "body": ["Wickid", "Jordi"],
        },
    },
    "shanks": {
        "label": "Shanks",
        "slots": {
            "beak": ["Radiant", "Flare", "Ashfire", "Boneblade"],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": ["Onyx", "Chernobyl", "Paleclaws", "Catriona"],
            "wings": ["Mandingo", "Helena"],
            "tail": ["Agave", "Rengoku"],
            "body": ["Badwitch", "Chummiest"],
        },
    },
    "levi": {
        "label": "Levi",
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
    "hybrid 2": {
        "label": "Hybrid 2",
        "slots": {
            "beak": [],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": [],
            "wings": ["Slenduh", "Awra"],
            "tail": [],
            "body": [],
        },
    },
    "hybrid 1": {
        "label": "Hybrid 1",
        "slots": {
            "beak": [],
            "comb": ["Yugi", "Super Sayang 1", "Killua", "Sakuragi"],
            "eyes": ["Diwata", "Maxx", "Retokada", "Yinyang"],
            "feet": [],
            "wings": [],
            "tail": [],
            "body": [],
        },
    },
}
