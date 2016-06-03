import datetime
import json
import os
from typing import List

import jsonpickle
import luigi
import sqlalchemy
from luigi.contrib import sqla
from luigi.mock import MockTarget
from sqlalchemy import String

import arrayexpress
import ena.credentials
import ena.ftp
import ena.metadata
import ena.submit
import urls


class EnaTaskException(Exception):
    pass


class SubmitAllSpecies(luigi.Task):
    """Spawns a SubmitSpecies task for every plant species."""
    limit = luigi.IntParameter(default=0)

    def run(self):
        species_list = arrayexpress.get_cram_species()
        if self.limit != 0:
            species_list = species_list[:self.limit]
        yield [SubmitSpecies(species, test=False) for species in species_list]


class SubmitSpecies(luigi.Task):
    """Submit all CRAM files for species to ENA."""
    species = luigi.Parameter()
    limit = luigi.IntParameter(default=0)
    test = luigi.BoolParameter(default=False)

    def requires(self):
        return ListCrams(self.species)

    def run(self):
        with self.input().open('r') as in_file:
            cram_list = jsonpickle.decode(in_file.read())  # type: List[arrayexpress.Cram]
        if self.limit != 0:
            cram_list = cram_list[:self.limit]

        tasks = []
        for c in cram_list:
            tasks.append(StoreEnaSubmissionResult(self.species, c.study_id, c.sample_ids, c.biorep_id,
                                                  c.run_ids, c.assembly_used, c.ftp_location, self.test))
        yield tasks
        with self.output().open('w') as out_file:
            out_file.write('done')

    def output(self):
        return MockTarget('submit_species_' + self.species)


class ListCrams(luigi.Task):
    """Query arrayexpress for a list of all CRAM files for species."""
    species = luigi.Parameter()
    resources = {'arrayexpress_rest_api': 1}

    def output(self):
        return MockTarget(self.species + '_crams')

    def run(self):
        cram_list = arrayexpress.get_cram_metadata(self.species)
        with self.output().open('w') as out_file:
            out_file.write(_json_pickle(cram_list))


def _json_pickle(x) -> str:
    json_ = jsonpickle.encode(x)
    parsed = json.loads(json_)
    pretty_json = json.dumps(parsed, indent=4)  # reformat json to look pretty
    return pretty_json


class StoreEnaSubmissionResult(sqla.CopyToTable):
    """Store the outcome of submission to ENA in an SQLite database."""
    species = luigi.Parameter()
    study_id = luigi.Parameter()
    sample_ids = luigi.ListParameter()
    biorep_id = luigi.Parameter()
    run_ids = luigi.ListParameter()
    assembly_used = luigi.Parameter()
    ftp_location = luigi.Parameter()
    test = luigi.BoolParameter()

    columns = [
        (['biorep_id', String(512)], {'primary_key': True}),
        (['species', String(128)], {}),
        (['ftp_location', String(1024)], {}),
        (['remote_md5', String(32)], {}),
        (['submission_acc', String(128)], {}),
        (['analysis_acc', String(128)], {})
    ]
    table = 'EnaSubmissionResult'
    sqlite_path = urls.sqlite
    connection_string = 'sqlite://'  # in-memory database
    if sqlite_path:
        connection_string += '/' + sqlite_path

    def requires(self):
        return SubmitToEna(self.species, self.study_id, self.sample_ids, self.biorep_id,
                           self.run_ids, self.assembly_used, self.ftp_location, self.test)

    def copy(self, conn, ins_rows, table_bound):
        bound_cols = dict((c, sqlalchemy.bindparam("_" + c.key)) for c in table_bound.columns)
        inserter = table_bound.insert().prefix_with("OR IGNORE")
        ins = inserter.values(bound_cols)
        conn.execute(ins, ins_rows)


class SubmitToEna(luigi.Task):
    """Submit the CRAM file under ftp_location to ENA and output the outcome."""
    species = luigi.Parameter()
    study_id = luigi.Parameter()
    sample_ids = luigi.ListParameter()
    biorep_id = luigi.Parameter()
    run_ids = luigi.ListParameter()
    assembly_used = luigi.Parameter()
    ftp_location = luigi.Parameter()
    test = luigi.BoolParameter()

    resources = {'ena_submission_endpoint': 1}

    def requires(self):
        return UploadCramToENA(self.ftp_location)

    def output(self):
        return MockTarget(self.biorep_id)

    def run(self):
        remote_md5 = arrayexpress.fetch_ftp_cram_md5(self.ftp_location)

        basic, references = self._create_metadata(remote_md5)
        response = ena.submit.submit_to_ena(basic, references, test=self.test)
        if not response.successful:
            if 'already exists as accession' in response.error:
                submission_acc = response.error.split(' ')[-1]
                if self.test:
                    # we won't be able to find any analysis ids on the public API if we made a test submission, skip
                    analysis_acc = ''
                else:
                    analysis_acc = arrayexpress.get_ena_analysis_id(submission_acc)
                response = ena.submit.Response(True, submission_acc, analysis_acc, error='')
            else:
                raise EnaTaskException(response.error)
        with self.output().open('w') as out_file:
            out_file.write('\t'.join(
                [self.biorep_id, self.species, self.ftp_location, remote_md5,
                 response.submission_acc, response.analysis_acc]))

    def _create_metadata(self, remote_md5):
        description = arrayexpress.fetch_cram_description(self.ftp_location)
        if len(self.run_ids) == 1:
            run_ids_string = self.run_ids[0]
        else:
            run_ids_string = ', '.join(id_ for id_ in self.run_ids)
        title = 'Alignment of %s to %s' % (run_ids_string, self.assembly_used)
        alias = self.biorep_id + '_cram_' + datetime.datetime.now().strftime('%Y-%m-%d')
        basic = ena.metadata.Basic(alias, 'EBI', 'ENSEMBL GENOMES',
                                   title, description, self.ftp_location, remote_md5)
        references = ena.metadata.References('ERP014374', self.sample_ids, self.assembly_used, self.run_ids)
        return basic, references


class UploadCramToENA(luigi.Task):
    """Upload CRAM file under ftp_location to ENA."""
    ftp_location = luigi.Parameter()

    def run(self):
        ena.ftp.upload_to_ena(self.ftp_location)

    def complete(self):
        # instead of opening a connection for each file, use cached result
        file_name = os.path.basename(self.ftp_location)
        return ena.ftp.is_present(file_name)
