from typing import List

from odm2_postgres_api.schemas.schemas import ControlledVocabularyCreate

cvs = [
    {
        "term":
        "trackSeriesCoverage",
        "name":
        "Track series coverage",
        "definition":
        "A series of ResultValues for a single Variable, measured with a moving platform or some sort of "
        "variable location, using a single Method, with specific Units, having a specific ProcessingLevel, "
        "and measured over time.",
        "category":
        "Coverage",
        "controlled_vocabulary_table_name":
        "cv_resulttype"
    },
    {
        "term": "taxonomicClassifierAnnotation",
        "name": "Taxonomic classifier annotation",
        "definition": "An annotation or qualifying comment about an TaxonomicClassifier.",
        "category": "Annotation",
        "controlled_vocabulary_table_name": "cv_annotationtype"
    },
]


def controlled_vocabularies() -> List[ControlledVocabularyCreate]:
    return [ControlledVocabularyCreate(**cv) for cv in cvs]
