/*
 *  Copyright © 2017-2019 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.directives.parser;

import io.cdap.wrangler.TestingRig;
import io.cdap.wrangler.api.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Tests {@link HL7Parser}
 */
public class HL7ParserTest {

  // Admit a patient.
  String adtA01 = "MSH|^~\\&|HIS|RIH|EKG|EKG|199904140038||ADT^A01||P|2.2\r"
    + "PID|0001|00009874|00001122|A00977|SMITH^JOHN^M|MOM|19581119|F|NOTREAL^LINDA^M|C|"
    + "564 SPRING ST^^NEEDHAM^MA^02494^US|0002|(818)565-1551|(425)828-3344|E|S|C|0000444444|252-00-4414||||SA|||SA||||"
    + "NONE|V1|0001|I|D.ER^50A^M110^01|ER|P00055|11B^M011^02|070615^BATMAN^GEORGE^L|555888^NOTREAL^BOB^K^DR^MD|"
    + "777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^NOTREAL^BILL^L|ER|000001916994|D|"
    + "|||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|199904101200|"
    + "|||5555112333|||666097^NOTREAL^MANNY^P\r"
    + "NK1|0222555|NOTREAL^JAMES^R|FA|STREET^OTHER STREET^CITY^ST^55566|(222)111-3333|(888)999-0000||||||"
    + "|ORGANIZATION\r"
    + "PV1|0001|I|D.ER^1F^M950^01|ER|P000998|11B^M011^02|070615^BATMAN^GEORGE^L|555888^OKNEL^BOB^K^DR^MD|"
    + "777889^NOTREAL^SAM^T^DR^MD^PHD|ER|D.WT^1A^M010^01|||ER|AMB|02|070615^VOICE^BILL^L|ER|000001916994|D|"
    + "|||||||||||||||GDD|WA|NORM|02|O|02|E.IN^02D^M090^01|E.IN^01D^M080^01|199904072124|199904101200|"
    + "||||5555112333|||666097^DNOTREAL^MANNY^P\r"
    + "PV2|||0112^TESTING|55555^PATIENT IS NORMAL|NONE|||19990225|19990226|1|1|TESTING|555888^NOTREAL^BOB^K^DR^MD|"
    + "|||||||||PROD^003^099|02|ER||NONE|19990225|19990223|19990316|NONE\r"
    + "AL1||SEV|001^POLLEN\r"
    + "GT1||0222PL|NOTREAL^BOB^B||STREET^OTHER STREET^CITY^ST^77787|(444)999-3333|(222)777-5555||||MO|111-33-5555|"
    + "|||NOTREAL GILL N|STREET^OTHER STREET^CITY^ST^99999|(111)222-3333\r"
    + "IN1||022254P|4558PD|BLUE CROSS|STREET^OTHER STREET^CITY^ST^00990||(333)333-6666||221K|LENIX|||19980515|19990515|"
    + "||PATIENT01 TEST D||||||||||||||||||02LL|022LP554";

  // Update patient info.
  String adt08 = "MSH|^~\\&|ADT|CHMC|ProAccess||20230822181701||ADT^A08|MT142756636134091|P|2.3|||AL|NE\r"
    + "PID|1|CEUL1984055|100003^^^&2.16.840.1.113883.3.1009&ISO~011806^^^San Luis Valley Reg Med Center&"
    + "2.16.840.1.113883.3.930&ISO~300713^^^San Luis Valley Reg Med Center&2.16.840.1.113883.3.930&ISO~CL0001115542^"
    + "^^CO Laboratory Services CL&&ISO~5354437^^^University of Colorado Health&2.16.840.1.113883.3.2889&ISO|RM680655|"
    + "BRONK^MARIANNA^|^^|19230324|F||2131-1|6999 UNIVERSITY PARK^^FEDERAL BOXES^CA^81125^^^||(408)-278-5231||EN|UNK|"
    + "VAR|515-55-4543|515-55-4543||||||||||\r"
    + "PD1||||PCPUN^Pcp^Unknown\r"
    + "NK1|1|POLASKI^BOBBY^|CHD|^^^^^^^|(408)-534-4829|\r"
    + "NK1|2|TYRIE^BLAIR^|CHD|^^^^^^^|(408)-752-1465|\r"
    + "PV1|1|I|MC3WEST1^MC3706^1|C|||BUKOAL^Bukowski^Allison^Marie|BUKOAL^Bukowski^Allison^Marie|"
    + "GIRAST^Girard^Stephen^Michael|HOSP||||1|||BUKOAL^Bukowski^Allison^Marie|IN||MCR|"
    + "||||||||||||||||||MC|||||202308211705\r"
    + "GT1|1||BEATO^RAYMOND^||1826 LAUPPE LANE^^CAMERON^NM^81125^^^|(408)-283-1928|||||SLF|828-46-4375|"
    + "|||INFORMATION UNAVAILABLE\r"
    + "PV2||REG|^SBO\r"
    + "AL1|1|MA|0030^.OSA^CODED|||20160822\r"
    + "AL1|2|DA|F001000476^Penicillins^CODED|||20160822\r"
    + "AL1|3|DA|F006000658^lisinopril^CODED|||20160822\r"
    + "AL1|4|DA|F006001550^codeine^CODED|||20160822\r"
    + "GT1|2||IFFLAND^JAMIE^||7267 FOREST HILLS DR^^SAN JOSE^CA^81125^^^|(408)-869-3802|||||SLF|390-23-9821|"
    + "|||INFORMATION UNAVAILABLE\r"
    + "IN1|1||MCR|MEDICARE PART A AND B|PO BOX 3113^^MECHANICSBURG^PA^17055-1828||(408)-913-3814|588-22-7099|NA||"
    + "INFORMATION UNAVAILABLE|||||DEMERIS^IRVING^|SLF|19230324|6632 DONEVA AVE^^LACROSSE^WA^81125^^^|"
    + "||||||||||||||||523748060A|||||||F||||||523748060A\r"
    + "IN2|1|515-55-4543|||||||||||||||||||||||||||||||||||||||||U||||||||||||||||||||(408)-278-5231\r"
    + "IN1|2||BCP|BC PPO COLORADO|PO BOX 5747^^DENVER^CO^80217-5747||(408)-905-0350|311-16-3529|MEDICARE SUPPLEMENT|"
    + "|INFORMATION UNAVAILABLE|||||MARYOTT^JESSE^|SLF|19230324|4564 WEST RIVER DR^^HONOLULU^HI^81125^^^|"
    + "||||||||||||||||PQC109A22034|||||||F||||||PQC109A22034\r"
    + "IN2|2|515-55-4543|||||||||||||||||||||||||||||||||||||||||U||||||||||||||||||||(408)-278-5231";

  // Register a patient.
  String adt04 = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|"
    + "20110613083617||ADT^A04|934576120110613083617|P|2.3||||\r"
    + "EVN|A04|20110613083617|||\r"
    + "PID|1||135769||MOUSE^MICKEY^||19281118|M|||123 Main St.^^Lake Buena Vista^FL^32830|"
    + "|(407)939-1289^^^theMainMouse@disney.com|||||1719|99999999||||||||||||||||||||\r"
    + "PV1|1|O|||||7^Disney^Walt^^MD^^^^|||||||||||||||||||||||||||||||||||||||||||||";

  // Success
  String success = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|"
    + "20110614075841||ACK|1407511|P|2.3||||||\r"
    + "MSA|AA|1407511|Success||";

  // Error
  String error = "MSH|^~\\&|SENDING_APPLICATION|SENDING_FACILITY|RECEIVING_APPLICATION|RECEIVING_FACILITY|"
    + "20110614075841||ACK|1407511|P|2.3||||||\r"
    + "MSA|AE|1407511|Error processing record!||";

  // Orders
  String orders = "MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20120411070545||ORM^O01|59689|P|2.3\r"
    + "PID|1|12345|12345^^^MIE&1.2.840.114398.1.100&ISO^MR||MOUSE^MICKEY^S||19281118|M||"
    + "|123 Main St.^^Lake Buena Vista^FL^3283|||||||||||||||||||\r"
    + "PV1|1||7^Disney^Walt^^MD^^^^||||||||||||||||||||||||||||||||||||||||||||||"
    + "|^^^^^1.2.840.114398.1.668.11999116110119971081089799101||\r"
    + "IN1|1||1|ABC Insurance Medicaid|P O Box 12345^^Atlanta^GA^30348|Claims^Florida |(555)555-1234^^^^^^|G1234|"
    + "||||||G|Mouse^Mickey|SELF|19281118|123 Main St.^^Lake Buena Vista^FL^32830|Y||||||||||||P|"
    + "|||ZYX1234589-1|||||||M||||M||\r"
    + "ORC|NW|23|||Pending||^^^^^0||20150325170228|26^David^Dave||8^Selenium^Selenium|^^^^OFFICE^^^^^Office|"
    + "^^^test@email.com||||||||||\r"
    + "OBR|1|23||123^CREATININE|0|||||||||||8^Selenium^Selenium||||||||||||||||||||||||||||||||||\r"
    + "DG1|1|ICD|B60.0^BABESIOSIS^I10|BABESIOSIS||||||||||||\r"
    + "OBR|2|23||80061^LIPID PROFILE|0|||||||||||8^Selenium^Selenium||||||||||||||||||||||||||||||||||\r"
    + "DG1|1|ICD|B60.0^BABESIOSIS^I10|BABESIOSIS||||||||||||";

  // Results
  String results = "MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|201607060811||ORU^R03|5209141|D|2.3\r"
    + "PID|1|123456|123456||SPARROW^JACK^||19600101|M|||123 BLACK PEARL^^DETROIT^MI^48217|"
    + "|3138363978|||||1036557|123456789\r"
    + "PV1|||^^\r"
    + "ORC|RE||161810162||||00001^ONCE^^^^||201607060811|||00007553^PHYSICIAN^RICHARD^ ^^^||||||||"
    + "|BIOTECH CLINICAL LAB INC^^23D0709666^^^CLIA|25775 MEADOWBROOK^^NOVI^MI^48375|^^^^^248^9121700|"
    + "OFFICE^1234 MILE ROAD  SUITE # 2^ROYAL OAK^MI^48073\r"
    + "OBR|8|455648^LA01|1618101622^GROUP951|GROUP951^CHROMOSOMAL ANALYSIS^L|||201606291253|||||||201606291631|"
    + "|00007553^PHYSICIAN^RICHARD^ ^^^||||||201607060811|||F||1^^^^^9\r"
    + "OBX|1|ED|00008510^INTELLIGENT FLOW PROFILE^L||^^^^JVBERi0xLjQKJeLjz9MKCjEgMCBvYmoKPDwvVHlwZSAvQ2F0YWxvZwov"
    + "UGFnZXMgMiAwIFI+PgplbmRvYmoKCjIgMCBvYmoKPDwvVHlwZSAvUGFnZXMKL0tpZHMgWzMgMCBSXQovQ291bnQgMT4+CmVuZG9iagoKMTI"
    + "gMCBvYmoKPDwvTGVuZ3RoIDEzIDAgUgovRmlsdGVyIC9GbGF0ZURlY29kZT4+CnN0cmVhbQp4nM1c3ZPUOJJ/569QxMRFMLtgrG+buBfoHmZ6"
    + "BxgW+u5iN3hxV7m7feMqF3Y...T0YK||||||C|||201606291631\r"
    + "NTE|1|L|Reference Lab: GENOPTIX|L\r"
    + "NTE|2|L|2110 ROUTHERFORD RD|L\r"
    + "NTE|3|L|CARLSBAD, CA  92008|L";

  // Notes and comments
  String nte = "MSH|^~\\&|SendingApp|SendingFac|ReceivingApp|ReceivingFac|20150323144246||ORU^R01|91734|P|2.3\r"
    + "PID|1||6010022||SOFTTEST^HIECLIENT||19650601|F||||||||||60100222016\r"
    + "PV1|1|O|1893^^^ZHIE||||4098^Physician^Sheldon|4098^Physician^Sheldon|||||||||4098^Physician^Sheldon|R|"
    + "|||||||||||||||||||||||||201503201114\r"
    + "ORC|RE|528527425|56083|||||||||4098^Physician^Sheldon^A\r"
    + "OBR|1|528527425|CTG150000145|LAB6460^Gene Bone Marrow||201503201114|201503231500|||||||201503231629|"
    + "|4098^Physician^Sheldon^^^^MD||||||201503231637||CTG|F||1\r"
    + "OBX|1|TX|LAB6460^Gene Bone Marrow||||||||||||RWBH\r"
    + "NTE|1||Physician: SHELDON A PHYSICIAN, MD\r"
    + "NTE|2||  NPI: 1881654887\r"
    + "NTE|3||Location: Oakland Medical Group III\r"
    + "NTE|4||    Loc ID: 1893\r"
    + "NTE|5\r"
    + "NTE|6||Collected: 3/23/2015 3:00 PM Received: 3/23/2015\r"
    + "NTE|7|| 4:29 PM Lab      642000066\r"
    + "NTE|8||\r"
    + "NTE|9||        Order#:\r"
    + "NTE|10\r"
    + "NTE|11||Clinical History: Testing field shows all data\r"
    + "NTE|12|| typed\r"
    + "NTE|13||CTG-15-145\r"
    + "NTE|14||\r"
    + "NTE|15\r"
    + "NTE|16||Specimen Type:               Bone Marrow No. of\r"
    + "NTE|17|| Cells Counted:  20\r"
    + "NTE|18||Modal Chromosome Number:     46          No. of\r"
    + "NTE|19|| Cells Analyzed: 20\r"
    + "NTE|20||No. and/or Type of Cultures: ST\r"
    + "NTE|21|| Hypermodal Cells:      0\r"
    + "NTE|22||Staining Method:             GTL\r"
    + "NTE|23|| Hypomodal Cells:       0\r"
    + "NTE|24||Band Level:                  400-450     No. of\r"
    + "NTE|25|| Karyotypes:     3\r"
    + "NTE|26\r"
    + "NTE|27||KARYOTYPE:      46,XX[20]\r"
    + "NTE|28||\r"
    + "NTE|29\r"
    + "NTE|30||CYTOGENETIC DIAGNOSIS:        Normal Female\r"
    + "NTE|31|| Chromosome Complement\r"
    + "NTE|32\r"
    + "NTE|33||INTERPRETATION: Cytogenetic examination of twenty\r"
    + "NTE|34|| metaphase cells revealed a\r"
    + "NTE|35||normal female diploid karyotype with no\r"
    + "NTE|36|| consistent numerical or structural\r"
    + "NTE|37||chromosome aberrations observed. This chromosome\r"
    + "NTE|38|| study does not eliminate the\r"
    + "NTE|39||possibility of single gene defects, chromosomal\r"
    + "NTE|40|| mosaicism involving abnormal\r"
    + "NTE|41||cell lines of low frequency, small structural\r"
    + "NTE|42|| chromosome abnormalities, or\r"
    + "NTE|43||failure to sample any malignant clones(s) that\r"
    + "NTE|44|| may be present.\r"
    + "NTE|45\r"
    + "NTE|46||Test Person NODOC\r"
    + "NTE|47||\r"
    + "NTE|48||Medical Director, Clinical Cytogenomics\r"
    + "NTE|49|| Laboratory\r"
    + "NTE|50||Board Certified Clinical Cytogeneticist/Clinical\r"
    + "NTE|51|| Molecular Geneticist\r"
    + "NTE|52||3/23/2015 4:37 PM\r"
    + "NTE|53||\r"
    + "NTE|54||All results from conventional karyotyping,\r"
    + "NTE|55|| molecular cytogenetics (FISH), and\r"
    + "NTE|56||chromosome microarray studies, as well as the\r"
    + "NTE|57|| final report, have been reviewed\r"
    + "NTE|58||by a staff cytogeneticist.\r"
    + "NTE|59||";


  @Test
  public void testBasic() throws Exception {
    String[] directives = new String[] {
      "parse-as-hl7 body",
      "keep body_hl7_MSH",
    };

    List<Row> rows = Arrays.asList(
      new Row("body", adtA01),
      new Row("body", adt08),
      new Row("body", adt04),
      new Row("body", success),
      new Row("body", error),
      new Row("body", orders),
      new Row("body", results),
      new Row("body", nte)
    );

    // The best we can do is check if the message is parsed successfully.
    rows = TestingRig.execute(directives, rows);
    Assert.assertNotNull(rows);
  }

}
