import os
import unittest

from hamcrest import assert_that, greater_than, starts_with, is_, has_items, has_length, contains_string

import arrayexpress
from arrayexpress import ArrayexpressException


class ArrayexpressTest(unittest.TestCase):
    ftp_path = 'ftp://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/atlas/rnaseq/DRR000/DRR000746/DRR000746.cram'

    def test_get_cram_species(self):
        species = arrayexpress.get_cram_species()
        print(len(species))
        assert_that(len(species), greater_than(30))
        assert_that(species, has_items('oryza_sativa', 'arabidopsis_thaliana', 'zea_mays'))

    def test_get_cram_metadata(self):
        meta_list = arrayexpress.get_cram_metadata('oryza_sativa')
        assert_that(len(meta_list), greater_than(100))

        first = meta_list[0]
        assert_that(first.study_id, starts_with('DRP'))
        assert_that(first.sample_ids[0], starts_with('SAMD'))
        assert_that(first.biorep_id, starts_with('DRR'))
        assert_that(first.run_ids[0], starts_with('DRR'))
        assert_that(first.ftp_location, starts_with('ftp://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/atlas/rnaseq'))

    def test_get_invalid_cram_metadata(self):
        meta_list = arrayexpress.get_cram_metadata('spam')
        assert_that(meta_list, is_([]))

    def test_get_cram_md5(self):
        md5 = arrayexpress.fetch_ftp_cram_md5(self.ftp_path)
        assert_that(md5, is_('f89bcb043fab48221192d4152b9611ab'))

        with self.assertRaises(ArrayexpressException):
            arrayexpress.fetch_ftp_cram_md5('spam')

    @unittest.skip('this only works on the internal network of the EBI, '
                   'with direct access to the file system behind the public ftp site')
    def test_get_cram_md5_nfs(self):
        md5 = arrayexpress.fetch_ftp_cram_md5(self.ftp_path, use_nfs=True)
        assert_that(md5, is_('f89bcb043fab48221192d4152b9611ab'))

        with self.assertRaises(ArrayexpressException):
            arrayexpress.fetch_ftp_cram_md5('spam', use_nfs=True)

    @staticmethod
    def _check_cram_description(description):
        assert_that(description.split('\n'), has_length(5))
        assert_that(description, contains_string('QC'))
        assert_that(description, contains_string('Filtering and trimming'))
        assert_that(description, contains_string('htseq'))
        assert_that(description, contains_string('tophat2'))
        assert_that(description, contains_string('iRAP'))

    def test_get_cram_description(self):
        description = arrayexpress.fetch_cram_description(self.ftp_path)
        self._check_cram_description(description)

        with self.assertRaises(ArrayexpressException):
            arrayexpress.fetch_cram_description('spam')

    @unittest.skip('this only works on the internal network of the EBI, '
                   'with direct access to the file system behind the ArrayExpress ftp site')
    def test_get_cram_description_nfs(self):
        with self.assertRaises(ArrayexpressException):
            arrayexpress.fetch_cram_description('spam', use_nfs=True)

        description = arrayexpress.fetch_cram_description(self.ftp_path, use_nfs=True)
        self._check_cram_description(description)

    def test_get_ena_analysis_id(self):
        analysis_id = arrayexpress.get_ena_analysis_id('ERA534948')
        assert_that(analysis_id, is_('ERZ127729'))

        with self.assertRaises(ArrayexpressException):
            arrayexpress.get_ena_analysis_id('spam')
