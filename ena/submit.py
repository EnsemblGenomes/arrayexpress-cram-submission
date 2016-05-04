from xml.etree import ElementTree

import requests

import ena.create_xml as create_xml
import ena.credentials
import ena.metadata as metadata
import ena.urls


class Response:
    def __init__(self, successful: bool, submission_acc: str, analysis_acc: str, error: str):
        self.successful = successful
        self.submission_acc = submission_acc
        self.analysis_acc = analysis_acc
        self.error = error


def submit_to_ena(basic: metadata.Basic, references: metadata.References, test: bool = True) -> Response:
    submission_xml = create_xml.create_submission_xml(basic)
    analysis_xml = create_xml.create_analysis_xml(basic, references)
    if test:
        server = ena.urls.test_server
    else:
        server = ena.urls.production_server
    url = '{server}?auth=ena%20{user}%20{password}'.format(server=server, user=ena.credentials.user,
                                                           password=ena.credentials.password)

    files = [('SUBMISSION', ('submission.xml', submission_xml, 'text/xml')),
             ('ANALYSIS', ('analysis.xml', analysis_xml, 'text/xml'))]
    r = requests.post(url, files=files, verify=False)

    response = r.text
    tree = ElementTree.fromstring(response)
    if tree.get('success') == 'true':
        submission_acc = tree.find('SUBMISSION').get('accession')
        analysis_acc = tree.find('ANALYSIS').get('accession')
        return Response(True, submission_acc, analysis_acc, '')
    else:
        error_list = [e.text for e in tree.findall('.//ERROR')]
        error = '\n'.join(error_list)
        return Response(False, '', '', error)