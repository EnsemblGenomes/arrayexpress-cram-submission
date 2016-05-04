from typing import List


class Basic:
    def __init__(self, alias: str, center_name: str, broker_name: str,
                 title: str, description: str, ftp_location: str, md5: str):
        self.alias = alias
        self.center_name = center_name
        self.broker_name = broker_name
        self.title = title
        self.description = description
        self.file_path = ftp_location
        self.md5 = md5


class References:
    def __init__(self, study_accession: str, sample_accessions: List[str],
                 assembly_accession: str, run_accessions: List[str]):
        self.study_accession = study_accession
        self.sample_accessions = sample_accessions
        self.assembly_accession = assembly_accession
        self.run_accessions = run_accessions
