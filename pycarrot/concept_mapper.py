import pandas as pd

from .concept_finder import ConceptFinder


class ConceptMapper:
    def __init__(self, **kwargs):

        self.cf = ConceptFinder(**kwargs)
        self.concept_map = {}
        self.hde_map = {
            "gender": "person",
            "measurement": "measurement",
            "procedure": "procedure_occurrence",
            "condition": "condition_occurrence",
            "observation": "observation",
        }

    def set_scan_report(self, scan_report):
        self.scan_report = scan_report

    def get_term_mapping(self, df, col):
        retval = {}
        for _, row in df.iterrows():
            key = row[col]
            value = int(row["concept_id"])
            existing_value = retval.get(key, None)
            if existing_value:
                if not isinstance(existing_value, list):
                    retval[key] = [retval[key]]
                retval[key].append(value)
            else:
                retval[key] = value
        return retval

    def get_rules_conditions(self, source_table, person_id, date_event, conditions):

        df_conditions = self.df_mapped[self.df_mapped["domain_id"] == "Condition"]

        return {
            "condition_concept_id": {
                "source_table": source_table,
                "source_field": conditions,
                "term_mapping": self.get_term_mapping(df_conditions, conditions),
            },
            "condition_source_value": {
                "source_table": source_table,
                "source_field": conditions,
            },
            "condition_start_datetime": {
                "source_table": source_table,
                "source_field": date_event,
            },
            "person_id": {"source_table": source_table, "source_field": person_id},
        }

    def get_rules_person(self, source_table, person_id, date_event, source):

        df_person = self.df_mapped[self.df_mapped["domain_id"] == "Gender"]

        return {
            "gender_concept_id": {
                "source_table": source_table,
                "source_field": source,
                "term_mapping": self.get_term_mapping(df_person, source),
            },
            "gender_source_value": {
                "source_table": source_table,
                "source_field": source,
            },
            "birth_datetime": {
                "source_table": source_table,
                "source_field": date_event,
            },
            "person_id": {"source_table": source_table, "source_field": person_id},
        }

    def get_rules_procedure(self, source_table, person_id, date_event, source):

        df_procedure = self.df_mapped[self.df_mapped["domain_id"] == "Procedure"]

        return {
            "procedure_concept_id": {
                "source_table": source_table,
                "source_field": source,
                "term_mapping": self.get_term_mapping(df_procedure, source),
            },
            "procedure_source_value": {
                "source_table": source_table,
                "source_field": source,
            },
            "procedure_datetime": {
                "source_table": source_table,
                "source_field": date_event,
            },
            "person_id": {"source_table": source_table, "source_field": person_id},
        }

    def get_rules_measurement(self, source_table, person_id, date_event, source):

        df_measurement = self.df_mapped[self.df_mapped["domain_id"] == "Measurement"]

        return {
            "measurement_concept_id": {
                "source_table": source_table,
                "source_field": source,
                "term_mapping": self.get_term_mapping(df_measurement, source),
            },
            "measurement_source_value": {
                "source_table": source_table,
                "source_field": source,
            },
            "value_as_number": {"source_table": source_table, "source_field": source},
            "measurement_datetime": {
                "source_table": source_table,
                "source_field": date_event,
            },
            "person_id": {"source_table": source_table, "source_field": person_id},
        }

    def get_rules_observation(self, source_table, person_id, date_event, source):

        df_observation = self.df_mapped[self.df_mapped["domain_id"] == "Observation"]

        return {
            "observation_concept_id": {
                "source_table": source_table,
                "source_field": source,
                "term_mapping": self.get_term_mapping(df_observation, source),
            },
            "observation_source_value": {
                "source_table": source_table,
                "source_field": source,
            },
            "observation_datetime": {
                "source_table": source_table,
                "source_field": date_event,
            },
            "person_id": {"source_table": source_table, "source_field": person_id},
        }

    def get_rules(self, domain):
        if domain == "condition_occurrence":
            return self.get_rules_conditions
        elif domain == "person":
            return self.get_rules_person
        elif domain == "procedure_occurrence":
            return self.get_rules_procedure
        elif domain == "measurement":
            return self.get_rules_measurement
        elif domain == "observation":
            return self.get_rules_observation
        else:
            raise Exception(f"Unknown domain '{domain}'")

    def map(self, table, source, person_id, date_event):
        if not self.scan_report:
            raise Exception("scan_report not defined. Use mapper.set_scan_report()")

        df = self.scan_report[table].astype(str)

        if isinstance(source, dict):
            source, mapping = list(source.items())[0]
            name = table + ":" + source
            for field, concept_id in mapping.items():
                mapping[field] = self.cf.find_concept(concept_id, field)
            concept_map = mapping
        else:
            name = table + ":" + source
            codes = df[source].to_list()
            concept_map = self.cf.find(codes)

        self.concept_map[name] = concept_map

        df_mapped = (
            df.set_index(source)
            .join(
                pd.json_normalize(df[source].map(concept_map).explode()).set_index(
                    "original_code"
                )[["concept_id", "concept_name", "domain_id"]]
            )
            .reset_index()
            .dropna(subset="domain_id")
        )

        self.df_mapped = df_mapped

        rules = {}
        for domain in df_mapped["domain_id"].unique():
            domain = domain.lower()
            hde = self.hde_map[domain]
            rules[hde] = self.get_rules(hde)(table, person_id, date_event, source)

        return rules
