import os
import unittest
from xml.etree import ElementTree

import time
from hamcrest import assert_that, not_none, is_, starts_with

import ena.create_xml
import ena.ftp
import ena.metadata
import ena.submit


class EnaTest(unittest.TestCase):
    cram_path = os.path.join(os.path.dirname(__file__), 'resources', 'DRR000756_1k.cram')
    basic = ena.metadata.Basic('cram_test', 'EMBL-EBI - United Kingdom', 'ENSEMBL GENOMES', 'title', 'desc', cram_path,
                               '8882a5837a394eaeb35c2199badb8d29')
    references = ena.metadata.References(study_accession='DRP000315',
                                         sample_accessions=['SAMD00009892', 'SAMD00009893'],
                                         assembly_accession='assembly', run_accessions=['DRR000745', 'DRR000746'])

    def test_create_submission_xml(self):
        xml = ena.create_xml.create_submission_xml(self.basic)
        tree = ElementTree.fromstring(xml)
        assert_that(tree.tag, is_('SUBMISSION'))
        assert_that(tree.attrib['alias'], is_('cram_test'))
        assert_that(tree.attrib['center_name'], is_(self.basic.center_name))
        assert_that(tree.attrib['broker_name'], is_(self.basic.broker_name))

        add = tree.find('ACTIONS/ACTION/ADD')
        assert_that(add.attrib['source'], is_('analysis.xml'))
        assert_that(add.attrib['schema'], is_('analysis'))

    def test_create_analysis_xml(self):
        xml = ena.create_xml.create_analysis_xml(self.basic, self.references)
        tree = ElementTree.fromstring(xml)
        assert_that(tree.tag, is_('ANALYSIS'))
        assert_that(tree.attrib['alias'], is_('cram_test'))
        assert_that(tree.attrib['center_name'], is_(self.basic.center_name))
        assert_that(tree.attrib['broker_name'], is_(self.basic.broker_name))

        assert_that(tree.find('TITLE').text, is_('title'))
        assert_that(tree.find('DESCRIPTION').text, is_('desc'))
        assert_that(tree.find('STUDY_REF').attrib['accession'], is_(self.references.study_accession))

        sample_nodes = tree.findall('SAMPLE_REF')
        assert_that(len(sample_nodes), is_(2))
        sample_ids = [n.attrib['accession'] for n in sample_nodes]
        assert_that(sorted(sample_ids), is_(self.references.sample_accessions))

        run_nodes = tree.findall('RUN_REF')
        assert_that(len(run_nodes), is_(2))
        run_ids = [n.attrib['accession'] for n in run_nodes]
        assert_that(sorted(run_ids), is_(self.references.run_accessions))

        assert_that(tree.find('ANALYSIS_TYPE/REFERENCE_ALIGNMENT'), not_none())
        f = tree.find('FILES/FILE')
        assert_that(f.attrib['filename'], is_('DRR000756_1k.cram'))
        assert_that(f.attrib['filetype'], is_('cram'))
        assert_that(f.attrib['checksum_method'], is_('MD5'))
        assert_that(f.attrib['checksum'], is_(self.basic.md5))

    def test_ftp_upload(self):
        ena.ftp.upload_to_ena(self.cram_path)
        filename = os.path.basename(self.cram_path)
        time.sleep(5)
        assert_that(ena.ftp.is_present(filename), is_(True))
        assert_that(ena.ftp.is_present('spam.cram'), is_(False))

        ena.ftp._remove_from_ena(filename)
        time.sleep(5)
        assert_that(ena.ftp.is_present(filename), is_(False))

    @unittest.skip('the aspera client is only distributed as deb or rpm package,'
                   'tricky to install on a host without root')
    def test_ftp_upload_aspera(self):
        ena.ftp.upload_to_ena_aspera(self.cram_path)
        filename = os.path.basename(self.cram_path)
        time.sleep(5)
        assert_that(ena.ftp.is_present(filename), is_(True))
        assert_that(ena.ftp.is_present('spam.cram'), is_(False))
        ena.ftp._remove_from_ena(filename)

    def test_submit_to_ena(self):
        ena.ftp.upload_to_ena(self.cram_path)
        response = ena.submit.submit_to_ena(self.basic, self.references, test=True)

        # Submissions to the ena test server are deleted every 24 hours,
        # so only the first test submission of the day will go though.
        # Alternative: create unique alias and file name for every test invocation.
        if 'Submission with name cram_test already exists' not in response.error:
            assert_that(response.successful, is_(True))
            assert_that(response.submission_acc, starts_with('ERA'))
            assert_that(response.analysis_acc, starts_with('ERZ'))
            assert_that(response.error, is_(''))
        ena.ftp._remove_from_ena(os.path.basename(self.cram_path))
