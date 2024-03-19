from sqlalchemy import create_engine
from sqlalchemy import text, bindparam, String


class ConceptFinder:
    def __init__(self, username, password, hostname, port, database_name):

        connection_string = f"postgresql+psycopg2://{username}:{password}@{hostname}:{port}/{database_name}"
        self.engine = create_engine(connection_string)

        query = """
        SELECT c1.concept_code as original_code, c2.*
        FROM public."CONCEPT_RELATIONSHIP" cr
        JOIN public."CONCEPT" c1 ON cr.concept_id_1 = c1.concept_id
        JOIN public."CONCEPT" c2 ON cr.concept_id_2 = c2.concept_id
        WHERE c1.concept_code IN :concept_codes
        AND c1.vocabulary_id = :vocabulary_id
        AND cr.relationship_id = :relationship_id
        """

        self.stmt = text(query)
        self.stmt = self.stmt.bindparams(
            bindparam("concept_codes", expanding=True),
            bindparam("relationship_id", type_=String),
            bindparam("vocabulary_id", type_=String),
        )

    def _find(self, codes):
        params = {
            "concept_codes": codes,
            "relationship_id": "Maps to",
            "vocabulary_id": "ICD10CM",
        }
        with self.engine.connect() as conn:
            result = conn.execute(self.stmt, params).fetchall()
            if not result or len(result) < 1:
                return None
            column_names = result[0].keys()
            result_dicts = [dict(zip(column_names, row)) for row in result]
            retval = {}
            for r in result_dicts:
                code = r["original_code"]
                if code not in retval:
                    retval[code] = []
                retval[code].append(r)

            return retval

    def find_concept(self, concept_id, source):
        query = """
        SELECT *
        FROM public."CONCEPT" c
        WHERE c.concept_id = :concept_id
        """
        stmt = text(query)
        stmt = stmt.bindparams(
            bindparam("concept_id", type_=String),
        )
        params = {"concept_id": concept_id}
        with self.engine.connect() as conn:
            result = conn.execute(stmt, params).fetchall()
            column_names = result[0].keys()
            result_dicts = [dict(zip(column_names, row)) for row in result]
            retval = []
            for r in result_dicts:
                r["original_code"] = source
                retval.append(r)

            return retval

    def find(self, codes):
        if isinstance(codes, list):
            return self._find(codes)
        else:
            return self._find([codes])