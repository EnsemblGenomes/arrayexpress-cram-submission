import os
from io import StringIO

import ena.schema.SRA_analysis as analysis_schema
import ena.schema.SRA_submission as submission_schema
from ena import metadata


def create_submission_xml(basic: metadata.Basic) -> str:
    """Create the 'submission' XML document for programmatic submission to ENA, as described here:
    http://www.ebi.ac.uk/ena/submit/preparing-xmls#submission"""
    submission_set = submission_schema.SubmissionSetType()
    submission = submission_schema.SubmissionType(alias=basic.alias, center_name=basic.center_name,
                                                  broker_name=basic.broker_name)
    submission_set.add_SUBMISSION(submission)

    actions = submission_schema.ACTIONSType()
    add = submission_schema.ADDType(source='analysis.xml', schema='analysis')
    actions.add_ACTION(submission_schema.ACTIONType(ADD=add))
    submission.set_ACTIONS(actions)

    out = StringIO()
    submission_set.exportChildren(out, 1)
    return out.getvalue()


def create_analysis_xml(basic: metadata.Basic, ref: metadata.References) -> str:
    """Create the 'analysis' XML document for programmatic submission to ENA, as described here:
    http://www.ebi.ac.uk/ena/submit/preparing-xmls#cram_ref_seq_submissions_example"""
    analysis_set = analysis_schema.AnalysisSetType()
    analysis = analysis_schema.AnalysisType(alias=basic.alias, center_name=basic.center_name,
                                            broker_name=basic.broker_name)
    analysis_set.add_ANALYSIS(analysis)

    analysis.set_TITLE(basic.title)
    analysis.set_DESCRIPTION(basic.description)
    analysis.set_STUDY_REF(analysis_schema.STUDY_REFType(accession=ref.study_accession))
    for sample_accession in ref.sample_accessions:
        analysis.add_SAMPLE_REF(analysis_schema.SAMPLE_REFType(accession=sample_accession))

    for run_accession in ref.run_accessions:
        analysis.add_RUN_REF(analysis_schema.RUN_REFType(accession=run_accession))
    analysis.set_ANALYSIS_TYPE(
        analysis_schema.ANALYSIS_TYPEType(REFERENCE_ALIGNMENT=analysis_schema.ReferenceSequenceType()))

    file_name = os.path.basename(basic.file_path)
    files = analysis_schema.FILESType()
    files.add_FILE(analysis_schema.AnalysisFileType(filename=file_name, checksum=basic.md5, filetype='cram',
                                                    checksum_method='MD5'))
    analysis.set_FILES(files)

    out = StringIO()
    analysis_set.exportChildren(out, 1)
    return out.getvalue()
