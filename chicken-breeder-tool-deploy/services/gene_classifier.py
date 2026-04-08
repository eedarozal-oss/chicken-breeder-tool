from services.primary_build_classifier import classify_primary_build
from services.recessive_build_classifier import classify_recessive_build


def classify_gene_profile(chicken):
    primary_result = classify_primary_build(chicken)
    recessive_result = classify_recessive_build(chicken)

    return {
        **primary_result,
        **recessive_result,
    }
