import urllib
from typing import List
from xml.etree import ElementTree

import requests

import urls


class Cram:
    """Represents a single arrayexpress CRAM file and its metadata."""

    def __init__(self, study_id: str, sample_ids: List[str], biorep_id: str,
                 run_ids: List[str], assembly_used: str, ftp_location: str):
        self.study_id = study_id
        self.sample_ids = sample_ids
        self.biorep_id = biorep_id
        self.run_ids = run_ids
        self.assembly_used = assembly_used
        self.ftp_location = ftp_location


class ArrayexpressException(Exception):
    pass


def _cram_ftp_to_nfs_path(ftp_location: str) -> str:
    """Convert the public FTP server url to an EBI internal file path."""
    return ftp_location.replace('ftp://ftp.ebi.ac.uk/pub/databases/arrayexpress/data/atlas/rnaseq',
                                '/ebi/ftp/pub/databases/microarray/data/atlas/rnaseq')


def get_cram_species() -> List[str]:
    """Collect a list of all plant species form the arrayexpress rest endpoint."""
    r = requests.get(urls.arrayexpress + '/getOrganisms/plants')
    species_set = set(group['REFERENCE_ORGANISM'] for group in r.json())
    species = sorted(species_set)
    return species


def get_cram_metadata(species: str) -> List[Cram]:
    """Collect a list of all CRAM files and their metadata from the arrayexpress rest endpoint."""
    r = requests.get(urls.arrayexpress + '/getRunsByOrganism/' + species)
    meta_list = []
    for group in r.json():
        if group['STATUS'] == 'Complete':
            run_ids = group['RUN_IDS'].split(',')
            sample_ids = group['SAMPLE_IDS'].split(',')
            meta = Cram(group['STUDY_ID'], sample_ids, group['BIOREP_ID'], run_ids,
                        group['ASSEMBLY_USED'], group['FTP_LOCATION'])
            meta_list.append(meta)
    return meta_list


def fetch_ftp_cram_md5(ftp_path: str, use_nfs: bool = False) -> str:
    """Fetch the CRAM MD5 checksum from the arrayexpress FTP server."""
    md5_path = ftp_path + '.md5'
    try:
        if use_nfs:
            nfs_path = _cram_ftp_to_nfs_path(md5_path)
            response = open(nfs_path).read()
        else:
            response = urllib.request.urlopen(md5_path).read().decode()
    except (FileNotFoundError, ValueError):
        raise ArrayexpressException('MD5 for cram not present under path ' + md5_path) from None
    md5 = response.split(' ')[0]
    return md5


def fetch_cram_description(ftp_path: str, use_nfs: bool = False) -> str:
    """Fetch the CRAM pipeline description from the arrayexpress FTP server."""
    cram = ftp_path.split('/')[-1]
    path = ftp_path.replace(cram, 'irap.versions.tsv')
    try:
        if use_nfs:
            nfs_path = _cram_ftp_to_nfs_path(path)
            lines = open(nfs_path).readlines()
        else:
            lines = urllib.request.urlopen(path).readlines()
            lines = list(map(bytes.decode, lines))
    except (FileNotFoundError, ValueError):
        raise ArrayexpressException('description for cram not present under path ' + path)
    lines = list(map(str.strip, lines))

    # grep -P 'QC|Filtering and trimming|htseq|tophat2|iRAP' irap.versions.tsv
    interesting_starts = ['QC', 'Filtering and trimming',
                          'Pipeline	IRAP',
                          'Gene, transcript and exon quantification	htseq',
                          'Reads alignment	tophat2']
    is_interesting = lambda line: any([line.startswith(start) for start in interesting_starts])
    interesting_lines = list(filter(is_interesting, lines))
    description = '.\n'.join(interesting_lines)
    description = description.encode('ascii', 'replace').decode()
    return description


def get_ena_analysis_id(submission_id: str) -> str:
    """Fetch the submission id for an existing analysis id from the ENA rest endpoint."""
    r = requests.get('http://www.ebi.ac.uk/ena/data/view/%s&display=xml' % submission_id)
    if 'entry is not found' in r.text:
        raise ArrayexpressException("couldn't find analysis id for submission id " + submission_id)
    else:
        tree = ElementTree.fromstring(r.text)
        id_element = tree.find("SUBMISSION/SUBMISSION_LINKS/SUBMISSION_LINK/XREF_LINK[DB='ENA-ANALYSIS']/ID")
        if id_element is None:
            raise ArrayexpressException("couldn't find analysis xref id element in XML document")
        analysis_id = id_element.text
        return analysis_id
