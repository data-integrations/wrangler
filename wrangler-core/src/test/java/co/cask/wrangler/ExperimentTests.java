package co.cask.wrangler;

import co.cask.wrangler.api.DirectiveRegistry;
import co.cask.wrangler.api.Executor;
import co.cask.wrangler.api.RecipeParser;
import co.cask.wrangler.parser.GrammarBasedParser;
import co.cask.wrangler.parser.MigrateToV2;
import co.cask.wrangler.registry.CompositeDirectiveRegistry;
import co.cask.wrangler.registry.SystemDirectiveRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Class description here.
 */
public class ExperimentTests {

  private String getIdFromName(String name) {
    name = name.toLowerCase();
    name = name.replaceAll("[_ \t]+", "-");
    name = name.replaceAll("[/$%#@**&()!,~+=?><|}{]+", "");
    return name;
  }

  @Test
  public void testIdCreationFromName() throws Exception {
    String[] names = {
      "My Sample Recipe",
      "SSGT Transformation Recipe!",
      "{SSGT Transformation Recipe!}",
      "{SSGT Transformation Recipe!}<sample-file>",
      "test>???>>>>window",
      "test    test1",
      "window\t    \t   window1"
    };

    String[] expected = {
      "my-sample-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipe",
      "ssgt-transformation-recipesample-file",
      "testwindow",
      "test-test1",
      "window-window1"
    };

    for (int i = 0; i < names.length; ++i) {
      String name = names[i];
      String expect = expected[i];
      String id = getIdFromName(name);
      Assert.assertEquals(expect, id);
    }
  }

  @Test
  public void testCurrencyParsing() throws Exception {
    DirectiveRegistry registry = new CompositeDirectiveRegistry(
      new SystemDirectiveRegistry()
    );
    String directive = "#pragma load-directives ews-udd-datetime-formatter;\nfilter-rows-on condition-false body =^ \"202\"\nparse-as-fixed-length body  3,11,1,16,11,64,64,4,8,64,64,64,12,5,8,1,12,64,1,8,1,8,8,2,10,8,2,20,15,15,8,3,2,2,1,1,1,20,60,60,60,60,2,16,2,20,64,1,20,60,60,60,60,2,16,2,20,64,1,20,60,60,60,60,2,16,2,20,64,1,20,60,60,60,60,2,16,2,20,64,1,3,16,1,3,16,6,1,3,16,1,3,16,6,1,3,16,6,1,3,16,6,256,256,256,256,256,256,1,1,8,256,256,256,256,1,1,1,1,1,1,8,1,1,7,4,4,73,5,1,1,32 ' '\nset columns body,RECTYPE,RECTYPEDEF,RS_FILLER_1,COCODE,SSN,EMPL_ID,USER_ID,INHOUSENUM,PASOFDATE,FN,MN,LN,SUFFIX,TITLE,DFPIN,DIRECT_LOGIN,VERDIV,JOBTITLE,EESTATCD,MRHDATE,EESTATTYPE,ORIGHIREDATE,TERMDATE,TERMREASON,UCTERMREASON,LASTDAYWRKF,WRKSTATE,WRKLOCCD,FEIN,SUIACCTNUM,ADJHIREDATE,YRSOFSERV,MTHOFSERV,PAYFREQ,TWN_ADD,UCX_ADD,ADD_IND_1,TYPE_1,L1_1,L2_1,L3_1,CITY_1,STATE_1,ZIP_1,COUNTRY_1,COUNTY_1,PROVINCE_1,ADD_IND_2,TYPE_2,L1_2,L2_2,L3_2,CITY_2,STATE_2,ZIP_2,COUNTRY_2,COUNTY_2,PROVINCE_2,ADD_IND_3,TYPE_3,L1_3,L2_3,L3_3,CITY_3,STATE_3,ZIP_3,COUNTRY_3,COUNTY_3,PROVINCE_3,ADD_IND_4,TYPE_4,L1_4,L2_4,L3_4,CITY_4,STATE_4,ZIP_4,COUNTRY_4,COUNTY_4,PROVINCE_4,HOME_PHONE_IND,HOME_PHONE_COUNTRY,HOME_PHONE,WORK_PHONE_IND,WORK_PHONE_COUNTRY,WORK_PHONE,WORK_PHONE_EXT,CELL_PHONE_IND,CELL_PHONE_COUNTRY,CELL_PHONE,PAGER_IND,PAGER_COUNTRY,PAGER,PAGER_PIN,FAX_IND,FAX_COUNTRY,FAX,FAX_EXT,OTHER_IND,OTHER_PHONE_COUNTRY,OTHER_PHONE,OTHER_PHONE_EXT,EMAIL_1,EMAIL_2,EMAIL_3,EMAIL_4,EMAIL_5,EMAIL_6,MARITAL_STATUS,GENDER,BIRTH_DATE,EMPLOYER_DEF_1,EMPLOYER_DEF_2,EMPLOYER_DEF_3,EMPLOYER_DEF_4,EMPLOYER_LOG_DEF_1,EMPLOYER_LOG_DEF_2,EMPLOYER_LOG_DEF_3,EMPLOYER_LOG_DEF_4,IGNOREBP,CONSENT_FLAG,CONSENT_DATE,PRIMARY_EMAIL,SECONDARY_EMAIL,ADDRESS,DAYTIME_PHONE,EVENING_PHONE,RS_FILLER_2,TCI_ID,WOTC_ADD,TICS_ADD,TAX_GROUP_NAME\nset-column uuid '1515528025571'\nset-column filename 'C:\\\\DataFactory\\\\importfiles\\\\input\\\\14107\\\\14107_99999TRADE07232017100529TEV.v5'\nset-column headers '\"RECTYPE\",\"COCODE\",\"SSN\",\"EMPL_ID\",\"USER_ID\",\"INHOUSENUM\",\"PASOFDATE\",\"FN\",\"MN\",\"LN\",\"SUFFIX\",\"TITLE\",\"DFPIN\",\"DIRECT_LOGIN\",\"VERDIV\",\"JOBTITLE\",\"EESTATCD\",\"MRHDATE\",\"EESTATTYPE\",\"ORIGHIREDATE\",\"TERMDATE\",\"TERMREASON\",\"UCTERMREASON\",\"LASTDAYWRKF\",\"WRKSTATE\",\"WRKLOCCD\",\"FEIN\",\"SUIACCTNUM\",\"ADJHIREDATE\",\"YRSOFSERV\",\"MTHOFSERV\",\"PAYFREQ\",\"TWN_ADD\",\"UCX_ADD\",\"ADD_IND_1\",\"TYPE_1\",\"L1_1\",\"L2_1\",\"L3_1\",\"CITY_1\",\"STATE_1\",\"ZIP_1\",\"COUNTRY_1\",\"COUNTY_1\",\"PROVINCE_1\",\"ADD_IND_2\",\"TYPE_2\",\"L1_2\",\"L2_2\",\"L3_2\",\"CITY_2\",\"STATE_2\",\"ZIP_2\",\"COUNTRY_2\",\"COUNTY_2\",\"PROVINCE_2\",\"ADD_IND_3\",\"TYPE_3\",\"L1_3\",\"L2_3\",\"L3_3\",\"CITY_3\",\"STATE_3\",\"ZIP_3\",\"COUNTRY_3\",\"COUNTY_3\",\"PROVINCE_3\",\"ADD_IND_4\",\"TYPE_4\",\"L1_4\",\"L2_4\",\"L3_4\",\"CITY_4\",\"STATE_4\",\"ZIP_4\",\"COUNTRY_4\",\"COUNTY_4\",\"PROVINCE_4\",\"HOME_PHONE_IND\",\"HOME_PHONE_COUNTRY\",\"HOME_PHONE\",\"WORK_PHONE_IND\",\"WORK_PHONE_COUNTRY\",\"WORK_PHONE\",\"WORK_PHONE_EXT\",\"CELL_PHONE_IND\",\"CELL_PHONE_COUNTRY\",\"CELL_PHONE\",\"PAGER_IND\",\"PAGER_COUNTRY\",\"PAGER\",\"PAGER_PIN\",\"FAX_IND\",\"FAX_COUNTRY\",\"FAX\",\"FAX_EXT\",\"OTHER_IND\",\"OTHER_PHONE_COUNTRY\",\"OTHER_PHONE\",\"OTHER_PHONE_EXT\",\"EMAIL_1\",\"EMAIL_2\",\"EMAIL_3\",\"EMAIL_4\",\"EMAIL_5\",\"EMAIL_6\",\"MARITAL_STATUS\",\"GENDER\",\"BIRTH_DATE\",\"EMPLOYER_DEF_1\",\"EMPLOYER_DEF_2\",\"EMPLOYER_DEF_3\",\"EMPLOYER_DEF_4\",\"EMPLOYER_LOG_DEF_1\",\"EMPLOYER_LOG_DEF_2\",\"EMPLOYER_LOG_DEF_3\",\"EMPLOYER_LOG_DEF_4\",\"IGNOREBP\",\"CONSENT_FLAG\",\"CONSENT_DATE\",\"PRIMARY_EMAIL\",\"SECONDARY_EMAIL\",\"ADDRESS\",\"DAYTIME_PHONE\",\"EVENING_PHONE\",\"TCI_ID\",\"WOTC_ADD\",\"TICS_ADD\",\"TAX_GROUP_NAME\",\"PAY_PROCESS_BEGIN_DT\"' \ntrim  RECTYPE\ntrim  RECTYPEDEF\ntrim  RS_FILLER_1\ntrim  COCODE\ntrim  SSN\ntrim  EMPL_ID\ntrim  USER_ID\ntrim  INHOUSENUM\ntrim  PASOFDATE\ntrim  FN\ntrim  MN\ntrim  LN\ntrim  SUFFIX\ntrim  TITLE\ntrim  DFPIN\ntrim  DIRECT_LOGIN\ntrim  VERDIV\ntrim  JOBTITLE\ntrim  EESTATCD\ntrim  MRHDATE\ntrim  EESTATTYPE\ntrim  ORIGHIREDATE\ntrim  TERMDATE\ntrim  TERMREASON\ntrim  UCTERMREASON\ntrim  LASTDAYWRKF\ntrim  WRKSTATE\ntrim  WRKLOCCD\ntrim  FEIN\ntrim  SUIACCTNUM\ntrim  ADJHIREDATE\ntrim  YRSOFSERV\ntrim  MTHOFSERV\ntrim  PAYFREQ\ntrim  TWN_ADD\ntrim  UCX_ADD\ntrim  ADD_IND_1\ntrim  TYPE_1\ntrim  L1_1\ntrim  L2_1\ntrim  L3_1\ntrim  CITY_1\ntrim  STATE_1\ntrim  ZIP_1\ntrim  COUNTRY_1\ntrim  COUNTY_1\ntrim  PROVINCE_1\ntrim  ADD_IND_2\ntrim  TYPE_2\ntrim  L1_2\ntrim  L2_2\ntrim  L3_2\ntrim  CITY_2\ntrim  STATE_2\ntrim  ZIP_2\ntrim  COUNTRY_2\ntrim  COUNTY_2\ntrim  PROVINCE_2\ntrim  ADD_IND_3\ntrim  TYPE_3\ntrim  L1_3\ntrim  L2_3\ntrim  L3_3\ntrim  CITY_3\ntrim  STATE_3\ntrim  ZIP_3\ntrim  COUNTRY_3\ntrim  COUNTY_3\ntrim  PROVINCE_3\ntrim  ADD_IND_4\ntrim  TYPE_4\ntrim  L1_4\ntrim  L2_4\ntrim  L3_4\ntrim  CITY_4\ntrim  STATE_4\ntrim  ZIP_4\ntrim  COUNTRY_4\ntrim  COUNTY_4\ntrim  PROVINCE_4\ntrim  HOME_PHONE_IND\ntrim  HOME_PHONE_COUNTRY\ntrim  HOME_PHONE\ntrim  WORK_PHONE_IND\ntrim  WORK_PHONE_COUNTRY\ntrim  WORK_PHONE\ntrim  WORK_PHONE_EXT\ntrim  CELL_PHONE_IND\ntrim  CELL_PHONE_COUNTRY\ntrim  CELL_PHONE\ntrim  PAGER_IND\ntrim  PAGER_COUNTRY\ntrim  PAGER\ntrim  PAGER_PIN\ntrim  FAX_IND\ntrim  FAX_COUNTRY\ntrim  FAX\ntrim  FAX_EXT\ntrim  OTHER_IND\ntrim  OTHER_PHONE_COUNTRY\ntrim  OTHER_PHONE\ntrim  OTHER_PHONE_EXT\ntrim  EMAIL_1\ntrim  EMAIL_2\ntrim  EMAIL_3\ntrim  EMAIL_4\ntrim  EMAIL_5\ntrim  EMAIL_6\ntrim  MARITAL_STATUS\ntrim  GENDER\ntrim  BIRTH_DATE\ntrim  EMPLOYER_DEF_1\ntrim  EMPLOYER_DEF_2\ntrim  EMPLOYER_DEF_3\ntrim  EMPLOYER_DEF_4\ntrim  EMPLOYER_LOG_DEF_1\ntrim  EMPLOYER_LOG_DEF_2\ntrim  EMPLOYER_LOG_DEF_3\ntrim  EMPLOYER_LOG_DEF_4\ntrim  IGNOREBP\ntrim  CONSENT_FLAG\ntrim  CONSENT_DATE\ntrim  PRIMARY_EMAIL\ntrim  SECONDARY_EMAIL\ntrim  ADDRESS\ntrim  DAYTIME_PHONE\ntrim  EVENING_PHONE\ntrim  RS_FILLER_2\ntrim  TCI_ID\ntrim  WOTC_ADD\ntrim  TICS_ADD\ntrim  TAX_GROUP_NAME\nset-column PAY_PROCESS_BEGIN_DT \"\"\nset-column ErCode '14107'\nset-column cocode cocode.length() > 0 ? cocode : \"0\"\nset-type cocode int\nset-column ssn ssn.replaceAll(\"[-]\", \"\" )\nsend-to-error (!(ssn.trim() =~ \"(^\\d{3}\\d{2}\\d{4}$)|^\\d{9}\") )\nsend-to-error  ((ssn.trim() =~ \"^0.*\") || (ssn.trim() =~ \"^(000|666).*\"))\nsend-to-error ((ssn.trim() =~ [  '000000000',  '123456789',  '002281852',  '042103580',  '062360749',  '078051120',  '095073645',  '128036045',  '135016629',  '141186941',  '165167999',  '165187999',  '165207999',  '165227999',  '165247999',  '189092294',  '212097694',  '212099999',  '306302348',  '308125070',  '468288779',  '549241889']) )\nsend-to-error  (ssn.substring(0,3).trim()  == '000' || ssn.substring(3,5).trim()  == '00' || ssn.substring(5,9).trim()  == '0000')\news-udd-datetime-formatter :pasofdate \"yyyyMMdd\" \"yyyyMMdd\" true false\nset-column dfpin dfpin.length() > 0 ? dfpin : \"0\"\nsend-to-error (!(dfpin =~ \"[0-9]{4,8}\") )\nsend-to-error  (dfpin.trim() == '00000000')\nsend-to-error  (dfpin.substring(0,4).trim()  == '0000')\nset-type dfpin int\nsend-to-error  !dq:isEmpty(direct_login) && (!(direct_login.trim() =~ \"[YNyn]{1}\") )\nsend-to-error (!(eestatcd.trim().toLowerCase() =~ \"[2abcdefghijoqswabcdefghijoqsw]{1}\") )\news-udd-datetime-formatter :mrhdate \"yyyyMMdd\" \"yyyyMMdd\" true false\nsend-to-error  !dq:isEmpty(eestattype) && (!(eestattype.trim().toLowerCase() =~ \"[fpcstfpcst]{1}\") )\news-udd-datetime-formatter :orighiredate \"yyyyMMdd\" \"yyyyMMdd\" true false\news-udd-datetime-formatter :termdate \"yyyyMMdd\" \"yyyyMMdd\" true false\nsend-to-error  !dq:isEmpty(termreason) && (!(termreason.trim() =~ \"^0[1-6]{1}\") )\nsend-to-error  !dq:isEmpty(lastdaywrkf) && (!(lastdaywrkf.trim() =~ \"^0[1-6]{1}\") )\nset-column fein fein.length() > 0 ? fein : \"0\"\nsend-to-error (!(fein =~ \"^\\s{0,15}\\d{0,16}$\") )\nset-type fein int\n";
    String migrate = new MigrateToV2(directive).migrate();
    RecipeParser parser = new GrammarBasedParser(migrate, registry);
    parser.initialize(null);
    List<Executor> parse = parser.parse();
//    String[] directives = directive.split("\n");
//    CompileStatus compile = TestingRig.compile(directives);
    Assert.assertTrue(true);
  }

}
