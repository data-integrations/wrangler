# Parsing HL7 CCDA XML Files

This recipe shows using data prep directives to parse a HL7 CCDA XML file.


## Version

To paste this receipe as-is requires:

* Wrangler Service Artifact >= 1.1.0


## Sample Data

[Sample HL7 CCDA XML Data](sample/CCDA_R2_CCD_HL7.xml) can be used with this recipe.


## CDAP Pipeline

* CDAP Version 4.1.0 - [Parse HL7 CCDA XML Pipeline](pipelines/parse-hl7-ccda-xml.json)


## Recipe

To use this recipe in the Data Prep UI, import the sample data into a workspace.
If necessary, rename the column the data is in (`<column-name>`) to `body` using:

```
rename <column-name> body
```

You can now follow the remainder of the recipe:

```
// The CCDA XML file is incomplete and needs to be adjusted before it can be parsed as XML.
// This is a very common scenario.

find-and-replace body s/[\r\n]//g

// This XML file has multiple root elements.

split-to-rows body /ClinicalDocument>
set column body body+"/ClinicalDocument>"
extract-regex-groups body (<ClinicalDocument.*?ClinicalDocument>)
filter-rows-on empty-or-null-columns body_1_1
drop body
rename body_1_1 body

// Now it is ready to be parses as XML:

parse-as-xml body

// Use the `xpath` directive to extract only the required elements.
// Alternatively, use the `xpath-array` directive to extract an array of elements.
// Extract Custodian and Patient MRN

xpath body ASGN_AUTH_NM /ClinicalDocument/custodian/assignedCustodian/representedCustodianOrganization/name
xpath body MRN_ID /ClinicalDocument/PatientMRN

// Extract Patient information - Part 1

xpath body MRN_ID /ClinicalDocument/recordTarget/patientRole/patient/name/given
xpath body PTNT_FIRST_NM /ClinicalDocument/recordTarget/patientRole/patient/name/given
xpath body PTNT_LAST_NM /ClinicalDocument/recordTarget/patientRole/patient/name/family
xpath body PTNT_MIDDLE_NM /ClinicalDocument/recordTarget/patientRole/patient/name/middle
xpath body PTNT_SFX_NM /ClinicalDocument/recordTarget/patientRole/patient/name/suffix
xpath body PTNT_LN1_ADR /ClinicalDocument/recordTarget/patientRole/addr/streetAddressLine
xpath body PTNT_CITY_NM /ClinicalDocument/recordTarget/patientRole/addr/city
xpath body PTNT_ST_CD /ClinicalDocument/recordTarget/patientRole/addr/state
xpath body PTNT_ZIP_PLUS_4_CD /ClinicalDocument/recordTarget/patientRole/addr/postalCode

// Extract Patient information - Part 2

xpath body PTNT_BIRTH_DT_VAL /ClinicalDocument/recordTarget/patientRole/patient/birthTime/@value
xpath body PTNT_FIRST_NM_TP_CD /ClinicalDocument/recordTarget/patientRole/patient/name/given/@qualifier
xpath body PTNT_NM_TP_CD /ClinicalDocument/recordTarget/patientRole/patient/name/@use
xpath body PTNT_ADR_TP_CD /ClinicalDocument/recordTarget/patientRole/addr/@use
xpath body PTNT_GNDR_VAL /ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode/@code
xpath body PTNT_GNDR_DSC /ClinicalDocument/recordTarget/patientRole/patient/administrativeGenderCode/@displayName
xpath body PTNT_HO_PHN_NO /ClinicalDocument/recordTarget/patientRole/telecom[@use='HP']/@value
xpath body PTNT_WORK_PHN_NO /ClinicalDocument/recordTarget/patientRole/telecom[@use='WP']/@value
xpath body PTNT_MBL_PHN_NO /ClinicalDocument/recordTarget/patientRole/telecom[@use=' tel']/@value

// Extract Patient information - Part 3

xpath body PTNT_ETHN_ORIG_VAL /ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode/@code
xpath body PTNT_ETHN_ORIG_DSC /ClinicalDocument/recordTarget/patientRole/patient/ethnicGroupCode/@displayName
xpath body PTNT_RACE_VAL /ClinicalDocument/recordTarget/patientRole/patient/raceCode/@code
xpath body PTNT_RACE_DSC /ClinicalDocument/recordTarget/patientRole/patient/raceCode/@displayName
xpath body PTNT_PRI_LANG_VAL /ClinicalDocument/recordTarget/patientRole/patient/languageCommunication/languageCode/@code

// Extract Patient information - Part 4

xpath body PTNT_MRTL_STS_VAL /ClinicalDocument/recordTarget/patientRole/patient/maritalStatusCode/@code
xpath body PTNT_MRTL_STS_DSC /ClinicalDocument/recordTarget/patientRole/patient/maritalStatusCode/@displayName
xpath body PTNT_RELIG_VAL /ClinicalDocument/recordTarget/patientRole/patient/religiousAffiliationCode/@code
xpath body PTNT_RELIG_DSC /ClinicalDocument/recordTarget/patientRole/patient/religiousAffiliationCode/@displayName
xpath body PTNT_EMAIL_ADR /ClinicalDocument/recordTarget/patientRole/telecom[@use='HP']/@value

// Drop the body as it's no longer required:

drop body

// Optionally, you can specify the columns to keep:

keep ASGN_AUTH_NM,MRN_ID,PTNT_FIRST_NM,PTNT_LAST_NM,PTNT_MIDDLE_NM,PTNT_SFX_NM,PTNT_LN1_ADR,PTNT_CITY_NM,PTNT_ST_CD,PTNT_ZIP_PLUS_4_CD,PTNT_BIRTH_DT_VAL,PTNT_FIRST_NM_TP_CD,PTNT_NM_TP_CD,PTNT_ADR_TP_CD,PTNT_GNDR_VAL,PTNT_GNDR_DSC,PTNT_HO_PHN_NO,PTNT_WORK_PHN_NO,PTNT_MBL_PHN_NO,PTNT_ETHN_ORIG_VAL,PTNT_ETHN_ORIG_DSC,PTNT_RACE_VAL,PTNT_RACE_DSC,PTNT_PRI_LANG_VAL,PTNT_MRTL_STS_VAL,PTNT_MRTL_STS_DSC,PTNT_RELIG_VAL,PTNT_RELIG_DSC,PTNT_EMAIL_ADR:0

```
