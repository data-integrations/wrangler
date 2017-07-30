/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.wrangler.utils;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.ximpleware.IByteBuffer;
import com.ximpleware.VTDNav;
import org.w3c.dom.Document;

import java.io.InputStream;
import java.io.StringBufferInputStream;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/**
 * Utility class which provides functionality around XPaths, such as returning a list of XPaths that are applicable
 * to a given VTDNav object.
 */
public final class XPathUtil {

  private XPathUtil() { }

  /**
   * Returns a list of XPATHs that are applicable to the given VTDNav object.
   */
  public static Set<String> getXMLPaths(VTDNav vn) throws Exception {
    IByteBuffer iByteBuffer = vn.getXML();
    String xmlString = Bytes.toString(iByteBuffer.getBytes());

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();

    factory.setNamespaceAware(true);
    factory.setValidating(true);
    InputStream styleSheetStream =
      Thread.currentThread().getContextClassLoader().getResourceAsStream("stylesheet.xml");

    DocumentBuilder builder = factory.newDocumentBuilder();
    Document document = builder.parse(new StringBufferInputStream(xmlString));

    // Use a Transformer for output
    TransformerFactory tFactory = TransformerFactory.newInstance();
    StreamSource stylesource = new StreamSource(styleSheetStream);
    Transformer transformer = tFactory.newTransformer(stylesource);

    DOMSource source = new DOMSource(document);

    StringWriter stringWriter = new StringWriter();
    StreamResult result = new StreamResult(stringWriter);
    transformer.transform(source, result);


    Set<String> xmlPaths = Sets.newLinkedHashSet(Splitter.on("\n").split(stringWriter.toString()));

    // Because of how the XML transformer and the stylesheet produce their output, we need to replace the following
    // two xpaths
    // "/asds4_0:SabreASDS/stl15:GetReservationRS/stl15:Reservation/stl15:PassengerReservation[2]/stl15:Segments/stl15:Segment[2]/stl15:Air/stl15:OperatingAirlineCode"
    // "/asds4_0:SabreASDS/stl15:GetReservationRS/stl15:Reservation/stl15:PassengerReservation[1]/stl15:Segments/stl15:Segment[1]/stl15:Air/stl15:OperatingAirlineCode"
    // "/asds4_0:SabreASDS/stl15:GetReservationRS/stl15:Reservation/stl15:PassengerReservation/stl15:Segments/stl15:Segment/stl15:Air/stl15:ActionCode"
    // with the single xpath
    // "/asds4_0:SabreASDS/stl15:GetReservationRS/stl15:Reservation/stl15:PassengerReservation[*]/stl15:Segments/stl15:Segment[*]/stl15:Air/stl15:OperatingAirlineCode"
    while (true) {
      Set<String> replacementsNeeded = Sets.newLinkedHashSet();

      Set<String> set = new HashSet<>();
      for (String xmlPath : xmlPaths) {
        Matcher matcher = Pattern.compile("(.*?)(\\[\\d+?\\])(.*)").matcher(xmlPath);
        if (matcher.matches()) {
          replacementsNeeded.add(xmlPath.substring(0, matcher.end(1)));
          set.add(xmlPath.replaceFirst("\\[\\d+?\\]", "[*]"));
        } else {
          set.add(xmlPath);
        }
      }
      if (replacementsNeeded.isEmpty()) {
        break;
      }
      for (String replacementNeeded : replacementsNeeded) {
        Set<String> newXMLPaths = Sets.newLinkedHashSet();
        for (String xmlPath : set) {
          String newxmlPath = xmlPath;
          if (xmlPath.startsWith(replacementNeeded + "/")) {
            newxmlPath = xmlPath.replace(replacementNeeded, replacementNeeded + "[*]");
          }
          newXMLPaths.add(newxmlPath);
        }
        set = newXMLPaths;
      }
      xmlPaths = set;
    }

    return xmlPaths;
  }
}
