import pandas as pd


class HealthDataElement:
    destination_field_person = "person_id"
    destination_table = ""
    destination_field_datetime = ""
    destination_field_concept_id = ""
    destination_field_source = ""

    @classmethod
    def get_name(cls):
        return cls.destination_table

    @classmethod
    def get(cls, person_id, date_event, source_table, source_field, term_mapping):
        return {
            cls.destination_field_person: {
                "source_table": source_table,
                "source_field": person_id,
            },
            cls.destination_field_datetime: {
                "source_table": source_table,
                "source_field": date_event,
            },
            cls.destination_field_concept_id: {
                "source_table": source_table,
                "source_field": source_field,
                "term_mapping": term_mapping,
            },
            cls.destination_field_source: {
                "source_table": source_table,
                "source_field": source_field,
            },
        }


class ConditionOccurrence(HealthDataElement):
    destination_table = "condition_occurrence"
    destination_field_datetime = "condition_start_datetime"
    destination_field_concept_id = "condition_concept_id"
    destination_field_source = "condition_source_value"


class Person(HealthDataElement):
    destination_table = "person"
    destination_field_datetime = "birth_datetime"
    destination_field_concept_id = "gender_concept_id"
    destination_field_source = "gender_source_value"


class ProcedureOccurrence(HealthDataElement):
    destination_table = "procedure_occurrence"
    destination_field_datetime = "procedure_datetime"
    destination_field_concept_id = "procedure_concept_id"
    destination_field_source = "procedure_source_value"


class Measurement(HealthDataElement):
    destination_table = "measurement"
    destination_field_datetime = "measurement_datetime"
    destination_field_concept_id = "measurement_concept_id"
    destination_field_source = "measurement_source_value"


class Observation(HealthDataElement):
    destination_table = "observation"
    destination_field_datetime = "observation_datetime"
    destination_field_concept_id = "observation_concept_id"
    destination_field_source = "observation_source_value"


class ConceptMapper:
    def __init__(self, concept_finder):

        self.cf = concept_finder
        self.concept_map = {}
        self.hde_map = {
            "gender": Person,
            "measurement": Measurement,
            "procedure": ProcedureOccurrence,
            "condition": ConditionOccurrence,
            "observation": Observation,
        }

    def set_scan_report(self, scan_report):
        self.scan_report = scan_report

    def get_term_mapping(self, domain_name, source_field):
        df_domain = self.df_mapped[self.df_mapped["domain_id"] == domain_name]

        retval = {}
        for _, row in df_domain.iterrows():
            key = row[source_field]
            value = int(row["concept_id"])
            existing_value = retval.get(key, None)
            if existing_value:
                if not isinstance(existing_value, list):
                    retval[key] = [retval[key]]
                retval[key].append(value)
            else:
                retval[key] = value

        return retval

    def map(self, source_table, source_field, person_id, date_event, one_to_one=False):
        if not self.scan_report:
            raise Exception("scan_report not defined. Use mapper.set_scan_report()")

        df = self.scan_report[source_table].astype(str)

        if isinstance(source_field, dict):
            source_field, mapping = list(source_field.items())[0]
            name = source_table + ":" + source_field
            for field, concept_id in mapping.items():
                mapping[field] = self.cf.find_concept(concept_id, field)
            concept_map = mapping
        else:
            name = source_table + ":" + source_field
            codes = df[source_field].to_list()
            concept_map = self.cf.find(codes)

        self.concept_map[name] = concept_map

        df_mapped = (
            df.set_index(source_field)
            .join(
                pd.json_normalize(
                    df[source_field].map(concept_map).explode()
                ).set_index("original_code")[
                    ["concept_id", "concept_name", "domain_id"]
                ]
            )
            .reset_index()
            .dropna(subset="domain_id")
        )

        self.df_mapped = df_mapped

        rules = {}
        for domain_name in df_mapped["domain_id"].unique():
            hde = self.hde_map[domain_name.lower()]
            term_mapping = self.get_term_mapping(domain_name, source_field)

            rule = None
            if one_to_one == True:
                rule = []
                for source_value, mapped_values in term_mapping.items():
                    if not isinstance(mapped_values, list):
                        mapped_values = [mapped_values]
                    for mapped_value in mapped_values:
                        rule.append(
                            hde.get(
                                person_id,
                                date_event,
                                source_table,
                                source_field,
                                {source_value: mapped_value},
                            )
                        )
            else:
                rule = hde.get(
                    person_id, date_event, source_table, source_field, term_mapping
                )
            rules[hde.get_name()] = rule

        return rules
