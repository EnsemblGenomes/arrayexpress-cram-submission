import os
import unittest

import luigi
from hamcrest import assert_that, is_, starts_with, contains_string
from luigi.mock import MockTarget

from pipeline import UploadCramToENA, SubmitToEna, ListCrams, SubmitSpecies


class PipelineTest(unittest.TestCase):
    cram_path = os.path.join(os.path.dirname(__file__), 'resources', 'DRR000756.cram')

    def test_upload_task(self):
        task = UploadCramToENA(ftp_location=self.cram_path)
        has_completed = luigi.build([task], local_scheduler=True)
        assert_that(has_completed, is_(True))

    def test_broken_upload_task(self):
        task = UploadCramToENA(ftp_location='/spam')
        has_completed = luigi.build([task], local_scheduler=True)
        assert_that(has_completed, is_(False))

    def test_submit_task(self):
        task = SubmitToEna(species='oryza_sativa', study_id='DRP000315', sample_ids="['SAMD00009892', 'SAMD00009893']",
                           biorep_id='E-GEOD-35288', run_ids="['DRR000745', 'DRR000746']", assembly_used='IRGSP-1.0',
                           ftp_location='ftp://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/atlas/rnaseq/DRR000/DRR000749/DRR000749.cram',
                           test=True)
        has_completed = luigi.build([task], local_scheduler=True)
        assert_that(has_completed, is_(True))

        output = task.output().open('r').read()
        expected = '\t'.join(['E-GEOD-35288', 'oryza_sativa',
                              'ftp://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/atlas/rnaseq/DRR000/DRR000749/DRR000749.cram',
                              '87a7e7825909178fab1a4a07b5692dbc'])
        assert_that(output, starts_with(expected))
        assert_that(output, contains_string('\tERA'))

    def test_list_crams_task(self):
        task = ListCrams(species='oryza_sativa')
        has_completed = luigi.build([task], local_scheduler=True)
        assert_that(has_completed, is_(True))

    def test_submit_species_task(self):
        task = SubmitSpecies(species='oryza_sativa', limit=3, test=True)
        has_completed = luigi.build([task], local_scheduler=True)
        assert_that(has_completed, is_(True))

        # luigi.build() will return true if there were no scheduling errors,
        # we also have to check if the (compound) task completed by checking its output.
        output = MockTarget.fs.get_data('submit_species_oryza_sativa')
        assert_that(output, is_(b'done'))
