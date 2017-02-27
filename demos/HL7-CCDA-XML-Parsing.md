# HL7 CCDA XML

This recipe show you can use the wrangling directives to parse HL7 CCDA XML files.

## Version
To paste this receipe AS-IS, you would need

* Wrangler Service Artifact >= 1.1.0

## Sample Data

[Here](sample/CCDA_R2_CCD_HL7.xml) is the sample data for running these directives through.

## Recipe
```
// CCDA XML file is incomplete and needs to be fixed, before it can be parsed as XML
// This is very common scenario.

find-and-replace body s/[\r\n]//g

// This XML file has multiple root elements.

split-to-rows body /ClinicalDocument>
set column body body+"/ClinicalDocument>"
extract-regex-groups body (<ClinicalDocument.*?ClinicalDocument>)
drop body
rename body_1_1 body

// Now we are ready to parse as XML

parse-as-xml body

// Use XPATH directive to extract only the elements needs.
// Can also use XPATH-ARRAY to extract array of elements.
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

// Drop body as it's not needed.

drop body

// Optionally you can also do the following

keep ASGN_AUTH_NM,MRN_ID,PTNT_FIRST_NM,PTNT_LAST_NM,PTNT_MIDDLE_NM,PTNT_SFX_NM,PTNT_LN1_ADR,PTNT_CITY_NM,PTNT_ST_CD,PTNT_ZIP_PLUS_4_CD,PTNT_BIRTH_DT_VAL,PTNT_FIRST_NM_TP_CD,PTNT_NM_TP_CD,PTNT_ADR_TP_CD,PTNT_GNDR_VAL,PTNT_GNDR_DSC,PTNT_HO_PHN_NO,PTNT_WORK_PHN_NO,PTNT_MBL_PHN_NO,PTNT_ETHN_ORIG_VAL,PTNT_ETHN_ORIG_DSC,PTNT_RACE_VAL,PTNT_RACE_DSC,PTNT_PRI_LANG_VAL,PTNT_MRTL_STS_VAL,PTNT_MRTL_STS_DSC,PTNT_RELIG_VAL,PTNT_RELIG_DSC,PTNT_EMAIL_ADR:0

```
